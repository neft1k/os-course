#include "ema_join.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(const char* prog) {
  fprintf(stderr,"Использование: %s --left <файл> --right <файл> --out <файл> [--repeat N]\n",prog);
}

int main(int argc, char* argv[]) {
  const char* left_path = NULL;
  const char* right_path = NULL;
  const char* out_path = NULL;
  int repeat = 1;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--left") == 0 && i + 1 < argc) {
      left_path = argv[++i];
    } else if (strcmp(argv[i], "--right") == 0 && i + 1 < argc) {
      right_path = argv[++i];
    } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
      out_path = argv[++i];
    } else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
      repeat = atoi(argv[++i]);
      if (repeat <= 0) {
        fprintf(stderr, "--repeat должен быть больше нуля\n");
        return 1;
      }
    } else {
      usage(argv[0]);
      return 1;
    }
  }

  if (left_path == NULL || right_path == NULL || out_path == NULL) {
    usage(argv[0]);
    return 1;
  }

  size_t left_count = 0;
  size_t right_count = 0;
  Row* left = load_table(left_path, &left_count);
  if (left == NULL)
    return 1;
  Row* right = load_table(right_path, &right_count);
  if (right == NULL) {
    free(left);
    return 1;
  }

  JoinedRow* result = NULL;
  size_t result_count = 0;

  for (int i = 1; i <= repeat; ++i) {
    free(result);
    result = NULL;
    result_count = 0;

    struct timespec start = {0}, end = {0};
    clock_gettime(CLOCK_MONOTONIC, &start);
    result = nested_loop_join(left, left_count, right, right_count, &result_count);
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (result == NULL) {
      free(left);
      free(right);
      return 1;
    }

    printf("Итерация %d: совпадений=%zu время=%.6f с\n",i,result_count,seconds_between(&start, &end));
  }

  int rc = save_result(out_path, result, result_count);
  free(result);
  free(left);
  free(right);

  if (rc != 0)
    return 1;

  printf("Результат записан в %s\n", out_path);
  return 0;
}
