#include "io_load.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static bool parse_range(const char* text, off_t* start, off_t* end, bool* range_set) {
  const char* dash = strchr(text, '-');
  if (dash == NULL)
    return false;

  char left[32];
  char right[32];
  size_t left_len = (size_t)(dash - text);
  size_t right_len = strlen(dash + 1);
  if (left_len == 0 || right_len == 0 ||
      left_len >= sizeof(left) || right_len >= sizeof(right))
    return false;

  memcpy(left, text, left_len);
  left[left_len] = '\0';
  memcpy(right, dash + 1, right_len + 1);

  errno = 0;
  long long start_val = strtoll(left, NULL, 10);
  long long end_val = strtoll(right, NULL, 10);
  if (errno != 0 || start_val < 0 || end_val < 0)
    return false;

  *start = (off_t)start_val;
  *end = (off_t)end_val;
  *range_set = !(start_val == 0 && end_val == 0);
  return true;
}

void print_usage(const char* prog) {
  fprintf(stderr,
          "Usage: %s --rw <read|write> --block_size <bytes> --block_count <count>\n"
          "          --file <path> [--range start-end] [--direct on|off]\n"
          "          [--type sequence|random] [--repeat N]\n",
          prog);
}

int parse_args(int argc, char* argv[], options_t* opts) {
  opts->mode = MODE_READ;
  opts->block_size = 4096;
  opts->block_count = 1024;
  opts->path = NULL;
  opts->repeat = 1;
  opts->use_direct = false;
  opts->order = ORDER_SEQUENCE;
  opts->range_set = false;
  opts->range_start = 0;
  opts->range_end = 0;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--rw") == 0 && i + 1 < argc) {
      const char* mode = argv[++i];
      if (strcmp(mode, "read") == 0) {
        opts->mode = MODE_READ;
      } else if (strcmp(mode, "write") == 0) {
        opts->mode = MODE_WRITE;
      } else {
        fprintf(stderr, "Unknown --rw value: %s\n", mode);
        return -1;
      }
    } else if (strcmp(argv[i], "--block_size") == 0 && i + 1 < argc) {
      opts->block_size = (size_t)strtoull(argv[++i], NULL, 10);
      if (opts->block_size == 0) {
        fprintf(stderr, "block_size must be > 0\n");
        return -1;
      }
    } else if (strcmp(argv[i], "--block_count") == 0 && i + 1 < argc) {
      opts->block_count = (size_t)strtoull(argv[++i], NULL, 10);
      if (opts->block_count == 0) {
        fprintf(stderr, "block_count must be > 0\n");
        return -1;
      }
    } else if (strcmp(argv[i], "--file") == 0 && i + 1 < argc) {
      opts->path = argv[++i];
    } else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
      opts->repeat = atoi(argv[++i]);
      if (opts->repeat <= 0) {
        fprintf(stderr, "repeat must be > 0\n");
        return -1;
      }
    } else if (strcmp(argv[i], "--direct") == 0 && i + 1 < argc) {
      const char* val = argv[++i];
      if (strcmp(val, "on") == 0) {
        opts->use_direct = true;
      } else if (strcmp(val, "off") == 0) {
        opts->use_direct = false;
      } else {
        fprintf(stderr, "--direct accepts on/off\n");
        return -1;
      }
    } else if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
      const char* val = argv[++i];
      if (strcmp(val, "sequence") == 0) {
        opts->order = ORDER_SEQUENCE;
      } else if (strcmp(val, "random") == 0) {
        opts->order = ORDER_RANDOM;
      } else {
        fprintf(stderr, "--type accepts sequence/random\n");
        return -1;
      }
    } else if (strcmp(argv[i], "--range") == 0 && i + 1 < argc) {
      if (!parse_range(argv[++i], &opts->range_start, &opts->range_end, &opts->range_set)) {
        fprintf(stderr, "Invalid range format\n");
        return -1;
      }
    } else {
      fprintf(stderr, "Unknown argument: %s\n", argv[i]);
      return -1;
    }
  }

  if (opts->path == NULL) {
    fprintf(stderr, "--file is required\n");
    return -1;
  }

  return 0;
}
