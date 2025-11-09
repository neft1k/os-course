#include "io_load.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static void* allocate_buffer(size_t size) {
  void* buffer = malloc(size);
  if (buffer == NULL)
    fprintf(stderr, "malloc failed\n");
  return buffer;
}

static double seconds_between(const struct timespec* start,
                              const struct timespec* end) {
  time_t sec = end->tv_sec - start->tv_sec;
  long nsec = end->tv_nsec - start->tv_nsec;
  if (nsec < 0) {
    sec -= 1;
    nsec += 1000000000L;
  }
  return (double)sec + (double)nsec / 1e9;
}

static int check_range(const options_t* opts, const struct stat* st) {
  off_t range_start = opts->range_start;
  off_t range_end = opts->range_end;

  if (!opts->range_set) {
    range_start = 0;
    if (opts->mode == MODE_READ) {
      range_end = st->st_size;
    } else {
      range_end = range_start + (off_t)(opts->block_size * opts->block_count);
    }
  }

  if (opts->mode == MODE_READ && range_end > st->st_size) {
    fprintf(stderr, "Range exceeds file size for read mode\n");
    return -1;
  }

  if (range_end <= range_start) {
    fprintf(stderr, "Invalid range\n");
    return -1;
  }

  off_t span = range_end - range_start;
  if (opts->order == ORDER_SEQUENCE &&
      span < (off_t)(opts->block_size * opts->block_count)) {
    fprintf(stderr, "Range is too small for sequential access\n");
    return -1;
  }

  return 0;
}

static off_t pick_offset(const options_t* opts, off_t range_start, size_t index) {
  if (opts->order == ORDER_RANDOM) {
    off_t range_bytes = opts->range_end - opts->range_start;
    if (range_bytes < (off_t)opts->block_size)
      return opts->range_start;

    off_t slots = range_bytes / (off_t)opts->block_size;
    if (slots <= 0)
      slots = 1;
    off_t slot = (off_t)(rand() % (int)slots);
    return opts->range_start + slot * (off_t)opts->block_size;
  }

  return range_start + (off_t)(index * opts->block_size);
}

int run_io_workload(const options_t* opts) {
  int flags = (opts->mode == MODE_READ) ? O_RDONLY : (O_WRONLY | O_CREAT);
  int fd = open(opts->path, flags, 0666);
  if (fd < 0) {
    fprintf(stderr, "Failed to open %s: %s\n", opts->path, strerror(errno));
    return 1;
  }

#if defined(F_NOCACHE)
  if (opts->use_direct && fcntl(fd, F_NOCACHE, 1) != 0)
    fprintf(stderr, "fcntl failed: %s\n", strerror(errno));
#else
  if (opts->use_direct)
    fprintf(stderr, "Warning: F_NOCACHE not supported on this platform\n");
#endif

  struct stat st;
  if (fstat(fd, &st) != 0) {
    fprintf(stderr, "fstat failed: %s\n", strerror(errno));
    close(fd);
    return 1;
  }

  options_t local_opts = *opts;
  if (!local_opts.range_set) {
    local_opts.range_start = 0;
    if (local_opts.mode == MODE_READ)
      local_opts.range_end = st.st_size;
    else
      local_opts.range_end = local_opts.range_start + (off_t)(local_opts.block_size * local_opts.block_count);
  }

  if (check_range(&local_opts, &st) != 0) {
    close(fd);
    return 1;
  }

  void* buffer = allocate_buffer(local_opts.block_size);
  if (buffer == NULL) {
    close(fd);
    return 1;
  }

  if (local_opts.mode == MODE_WRITE) {
    for (size_t i = 0; i < local_opts.block_size; ++i)
      ((unsigned char*)buffer)[i] = (unsigned char)('A' + (i % 26));
  }

  srand(0);

  struct timespec total_start = {0}, total_end = {0};
  clock_gettime(CLOCK_MONOTONIC, &total_start);

  for (int r = 0; r < local_opts.repeat; ++r) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (size_t i = 0; i < local_opts.block_count; ++i) {
      off_t offset = pick_offset(&local_opts, local_opts.range_start, i);
      ssize_t done;
      if (local_opts.mode == MODE_READ) {
        done = pread(fd, buffer, local_opts.block_size, offset);
      } else {
        done = pwrite(fd, buffer, local_opts.block_size, offset);
      }
      if (done < 0) {
        fprintf(stderr, "I/O error at block %zu: %s\n", i, strerror(errno));
        free(buffer);
        close(fd);
        return 1;
      }
      if ((size_t)done != local_opts.block_size) {
        fprintf(stderr, "Short transfer at block %zu\n", i);
        free(buffer);
        close(fd);
        return 1;
      }
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    time_t sec = end.tv_sec - start.tv_sec;
    long nsec = end.tv_nsec - start.tv_nsec;
    if (nsec < 0) {
      sec -= 1;
      nsec += 1000000000L;
    }
    double elapsed = (double)sec + (double)nsec / 1e9;
    printf("Iteration %d: blocks=%zu size=%zu bytes time=%.6f s\n", r + 1, local_opts.block_count, local_opts.block_size, elapsed);
  }

  clock_gettime(CLOCK_MONOTONIC, &total_end);
  printf("Total time: %.6f s\n", seconds_between(&total_start, &total_end));

  free(buffer);
  close(fd);
  return 0;
}
