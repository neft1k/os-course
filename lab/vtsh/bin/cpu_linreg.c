#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#define XMIN 0.0
#define XMAX 100.0
#define A_TRUE 2.0
#define B_TRUE 3.0
#define NOISE_SIGMA 0.5

double rand01() {
  return (double)rand() / (double)RAND_MAX;
}

void run_linreg(long long n) {
  double mean_x = 0.0, mean_y = 0.0;
  double m2x = 0.0, cxy = 0.0;
  long long k = 0;

  for (long long i = 0; i < n; i++) {
    double x = XMIN + rand01() * (XMAX - XMIN);
    double noise = (rand01() - 0.5) * 2 * NOISE_SIGMA;
    double y = A_TRUE * x + B_TRUE + noise;

    k++;
    double dx = x - mean_x;
    double dy = y - mean_y;
    mean_x += dx / k;
    mean_y += dy / k;
    m2x += dx * (x - mean_x);
    cxy += dx * (y - mean_y);
  }

  double var_x = m2x / n;
  double cov_xy = cxy / n;

  double a = cov_xy / var_x;
  double b = mean_y - a * mean_x;

  printf("a = %.4f, b = %.4f\n", a, b);
}

int main(int argc, char *argv[]) {
  long long count = 10000000;
  int repeat = 3;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
      count = atoll(argv[++i]);
    } else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
      repeat = atoi(argv[++i]);
    } else {
      printf("Unknown option: %s\n", argv[i]);
      return 1;
    }
  }

  srand(7);

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int r = 1; r <= repeat; r++) {
    run_linreg(count);
  }

  clock_gettime(CLOCK_MONOTONIC, &end);
  double total_time = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
  printf("Total time = %.3f s\n", total_time);

  return 0;
}
