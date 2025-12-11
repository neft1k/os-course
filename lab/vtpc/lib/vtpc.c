#include "vtpc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define VTPC_MAX_FILES 128
#define VTPC_PAGE_CAPACITY 64

struct vtpc_page {
  off_t base;
  size_t valid;
  int dirty;
  int in_use;
  uint64_t last_access;
  char* data;
};

struct vtpc_file {
  int fd;
  int can_read;
  int can_write;
  int direct_io;
  off_t position;
  off_t file_size;
  size_t page_size;
  size_t capacity;
  uint64_t access_clock;
  struct vtpc_page* pages;
};

static struct vtpc_file* g_files[VTPC_MAX_FILES];

static size_t vtpc_get_page_size(void) {
  long page = sysconf(_SC_PAGESIZE);
  if (page <= 0)
    page = 4096;
  return (size_t)page;
}

static off_t vtpc_align_down(off_t value, size_t align) {
  off_t mod = value % (off_t)align;
  if (mod < 0)
    mod += (off_t)align;
  return value - mod;
}

static size_t vtpc_min_size(size_t a, size_t b) {
  return (a < b) ? a : b;
}

static size_t vtpc_max_size(size_t a, size_t b) {
  return (a > b) ? a : b;
}

static struct vtpc_file* vtpc_lookup(int handle) {
  if (handle < 0 || handle >= VTPC_MAX_FILES)
    return NULL;
  return g_files[handle];
}

static int vtpc_store(struct vtpc_file* file) {
  for (int i = 0; i < VTPC_MAX_FILES; ++i) {
    if (g_files[i] == NULL) {
      g_files[i] = file;
      return i;
    }
  }
  errno = EMFILE;
  return -1;
}

static void vtpc_drop(int handle) {
  if (handle < 0 || handle >= VTPC_MAX_FILES)
    return;
  g_files[handle] = NULL;
}

static int vtpc_open_raw(const char* path, int mode, int access, int* direct_io) {
  int fd = -1;
  *direct_io = 0;

#ifdef O_DIRECT
  fd = open(path, mode | O_DIRECT, access);
  if (fd >= 0) {
    *direct_io = 1;
    return fd;
  }
  if (errno != EINVAL && errno != EOPNOTSUPP)
    return fd;
#endif

  fd = open(path, mode, access);
  if (fd < 0)
    return fd;

#ifdef F_NOCACHE
  if (fcntl(fd, F_NOCACHE, 1) == 0)
    *direct_io = 1;
#endif

  return fd;
}

static int vtpc_alloc_pages(struct vtpc_file* file) {
  file->pages = calloc(file->capacity, sizeof(*file->pages));
  if (file->pages == NULL)
    return -1;

  for (size_t i = 0; i < file->capacity; ++i) {
    if (posix_memalign((void**)&file->pages[i].data, file->page_size, file->page_size) != 0) {
      for (size_t j = 0; j < i; ++j)
        free(file->pages[j].data);
      free(file->pages);
      file->pages = NULL;
      return -1;
    }
    memset(file->pages[i].data, 0, file->page_size);
  }
  return 0;
}

static struct vtpc_page* vtpc_find_page(struct vtpc_file* file, off_t base) {
  for (size_t i = 0; i < file->capacity; ++i) {
    if (file->pages[i].in_use && file->pages[i].base == base)
      return &file->pages[i];
  }
  return NULL;
}

static int vtpc_flush_page(struct vtpc_file* file, struct vtpc_page* page) {
  if (!page->in_use || !page->dirty)
    return 0;

  if (file->file_size <= page->base) {
    page->dirty = 0;
    return 0;
  }

  size_t len = (size_t)vtpc_min_size((size_t)(file->file_size - page->base), file->page_size);
  if (len == 0) {
    page->dirty = 0;
    return 0;
  }

  size_t write_len = len;
  if (file->direct_io)
    write_len = file->page_size;

  ssize_t written = pwrite(file->fd, page->data, write_len, page->base);
  if (written < 0 || (size_t)written != write_len)
    return -1;

#if defined(POSIX_FADV_DONTNEED)
  (void)posix_fadvise(file->fd, page->base, (off_t)write_len, POSIX_FADV_DONTNEED);
#endif

  if (write_len > len && ftruncate(file->fd, file->file_size) != 0)
    return -1;

  page->dirty = 0;
  return 0;
}

static struct vtpc_page* vtpc_pick_victim(struct vtpc_file* file) {
  struct vtpc_page* victim = NULL;
  for (size_t i = 0; i < file->capacity; ++i) {
    if (!file->pages[i].in_use)
      continue;
    if (victim == NULL || file->pages[i].last_access > victim->last_access)
      victim = &file->pages[i];
  }
  return victim;
}

static struct vtpc_page* vtpc_prepare_page(struct vtpc_file* file, off_t base) {
  struct vtpc_page* page = vtpc_find_page(file, base);
  if (page != NULL)
    return page;

  for (size_t i = 0; i < file->capacity; ++i) {
    if (!file->pages[i].in_use) {
      page = &file->pages[i];
      break;
    }
  }

  if (page == NULL) {
    page = vtpc_pick_victim(file);
    if (page == NULL) {
      errno = ENOMEM;
      return NULL;
    }
    if (vtpc_flush_page(file, page) != 0)
      return NULL;
  }

  page->in_use = 1;
  page->base = base;
  page->valid = 0;
  page->dirty = 0;
  page->last_access = 0;

  ssize_t done = pread(file->fd, page->data, file->page_size, page->base);
  if (done < 0) {
    page->in_use = 0;
    return NULL;
  }

#if defined(POSIX_FADV_DONTNEED)
  (void)posix_fadvise(file->fd, page->base, (off_t)file->page_size, POSIX_FADV_DONTNEED);
#endif

  page->valid = (size_t)done;
  if (page->valid < file->page_size)
    memset(page->data + page->valid, 0, file->page_size - page->valid);

  return page;
}

static int vtpc_flush_all(struct vtpc_file* file) {
  int has_dirty = 0;
  for (size_t i = 0; i < file->capacity; ++i) {
    if (file->pages[i].in_use && file->pages[i].dirty) {
      has_dirty = 1;
      if (vtpc_flush_page(file, &file->pages[i]) != 0)
        return -1;
    }
  }

  if (has_dirty && file->can_write) {
    if (ftruncate(file->fd, file->file_size) != 0)
      return -1;
  }

  if (fsync(file->fd) != 0)
    return -1;

  return 0;
}

int vtpc_open(const char* path, int mode, int access) {
  size_t page_size = vtpc_get_page_size();

  struct vtpc_file* file = calloc(1, sizeof(*file));
  if (file == NULL)
    return -1;

  file->page_size = page_size;
  file->capacity = VTPC_PAGE_CAPACITY;

  int direct_io = 0;
  int fd = vtpc_open_raw(path, mode, access, &direct_io);
  if (fd < 0) {
    free(file);
    return -1;
  }

  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    free(file);
    return -1;
  }

  file->fd = fd;
  file->file_size = st.st_size;
  file->position = 0;
  file->direct_io = direct_io;
  file->access_clock = 0;

  int accmode = mode & O_ACCMODE;
  file->can_read = (accmode == O_RDONLY || accmode == O_RDWR);
  file->can_write = (accmode == O_WRONLY || accmode == O_RDWR);

  if (vtpc_alloc_pages(file) != 0) {
    close(fd);
    free(file);
    errno = ENOMEM;
    return -1;
  }

  int handle = vtpc_store(file);
  if (handle < 0) {
    for (size_t i = 0; i < file->capacity; ++i)
      free(file->pages[i].data);
    free(file->pages);
    close(fd);
    free(file);
    return -1;
  }

  return handle;
}

int vtpc_close(int fd) {
  struct vtpc_file* file = vtpc_lookup(fd);
  if (file == NULL) {
    errno = EBADF;
    return -1;
  }

  int result = 0;
  if (vtpc_flush_all(file) != 0)
    result = -1;

  if (close(file->fd) != 0)
    result = -1;

  for (size_t i = 0; i < file->capacity; ++i)
    free(file->pages[i].data);
  free(file->pages);
  free(file);
  vtpc_drop(fd);
  return result;
}

ssize_t vtpc_read(int fd, void* buf, size_t count) {
  struct vtpc_file* file = vtpc_lookup(fd);
  if (file == NULL) {
    errno = EBADF;
    return -1;
  }
  if (!file->can_read) {
    errno = EBADF;
    return -1;
  }
  if (count == 0)
    return 0;

  size_t total = 0;
  while (total < count) {
    if (file->position >= file->file_size)
      break;

    off_t base = vtpc_align_down(file->position, file->page_size);
    size_t page_off = (size_t)(file->position - base);
    size_t max_in_page = file->page_size - page_off;

    size_t remaining = count - total;
    size_t available = (size_t)vtpc_min_size((size_t)(file->file_size - file->position), max_in_page);
    size_t chunk = vtpc_min_size(remaining, available);

    if (chunk == 0)
      break;

    struct vtpc_page* page = vtpc_prepare_page(file, base);
    if (page == NULL)
      return -1;

    page->last_access = ++file->access_clock;

    if (page->valid < page_off + chunk)
      chunk = (page->valid > page_off) ? (page->valid - page_off) : 0;

    if (chunk == 0)
      break;

    memcpy((char*)buf + total, page->data + page_off, chunk);
    total += chunk;
    file->position += (off_t)chunk;
  }

  return (ssize_t)total;
}

ssize_t vtpc_write(int fd, const void* buf, size_t count) {
  struct vtpc_file* file = vtpc_lookup(fd);
  if (file == NULL) {
    errno = EBADF;
    return -1;
  }
  if (!file->can_write) {
    errno = EBADF;
    return -1;
  }
  if (count == 0)
    return 0;

  size_t total = 0;
  while (total < count) {
    off_t base = vtpc_align_down(file->position, file->page_size);
    size_t page_off = (size_t)(file->position - base);
    size_t remaining = count - total;
    size_t chunk = vtpc_min_size(remaining, file->page_size - page_off);

    struct vtpc_page* page = vtpc_prepare_page(file, base);
    if (page == NULL)
      return -1;

    memcpy(page->data + page_off, (const char*)buf + total, chunk);
    page->valid = vtpc_min_size(file->page_size, vtpc_max_size(page->valid, page_off + chunk));
    page->dirty = 1;
    page->last_access = ++file->access_clock;

    total += chunk;
    file->position += (off_t)chunk;

    off_t new_end = base + (off_t)vtpc_max_size(page->valid, page_off + chunk);
    if (new_end > file->file_size)
      file->file_size = new_end;
  }

  return (ssize_t)total;
}

off_t vtpc_lseek(int fd, off_t offset, int whence) {
  struct vtpc_file* file = vtpc_lookup(fd);
  if (file == NULL) {
    errno = EBADF;
    return (off_t)-1;
  }

  off_t base = 0;
  if (whence == SEEK_SET) {
    base = offset;
  } else if (whence == SEEK_CUR) {
    base = file->position + offset;
  } else if (whence == SEEK_END) {
    base = file->file_size + offset;
  } else {
    errno = EINVAL;
    return (off_t)-1;
  }

  if (base < 0) {
    errno = EINVAL;
    return (off_t)-1;
  }

  file->position = base;
  return base;
}

int vtpc_fsync(int fd) {
  struct vtpc_file* file = vtpc_lookup(fd);
  if (file == NULL) {
    errno = EBADF;
    return -1;
  }

  return vtpc_flush_all(file);
}
