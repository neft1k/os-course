#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

typedef enum {
  MODE_READ,
  MODE_WRITE
} io_mode_t;

typedef enum {
  ORDER_SEQUENCE,
  ORDER_RANDOM
} io_order_t;

typedef struct {
  io_mode_t mode;
  size_t block_size;
  size_t block_count;
  const char* path;
  int repeat;
  bool use_direct;
  io_order_t order;
  bool range_set;
  off_t range_start;
  off_t range_end;
} options_t;

void print_usage(const char* prog);
int parse_args(int argc, char* argv[], options_t* opts);
int run_io_workload(const options_t* opts);
