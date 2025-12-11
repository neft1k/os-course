#include "io_load.h"

#include <stdio.h>

int main(int argc, char* argv[]) {
  options_t opts;
  if (parse_args(argc, argv, &opts) != 0) {
    print_usage(argv[0]);
    return 1;
  }

  if (run_io_workload(&opts) != 0)
    return 1;

  return 0;
}
