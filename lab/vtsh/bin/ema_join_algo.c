#include "ema_join.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

JoinedRow* nested_loop_join(const Row* left,size_t left_count,const Row* right,size_t right_count,size_t* out_count) {
  JoinedRow* result = NULL;
  size_t capacity = 0;
  size_t size = 0;

  for (size_t i = 0; i < left_count; ++i) {
    for (size_t j = 0; j < right_count; ++j) {
      if (left[i].id == right[j].id) {
        if (size == capacity) {
          size_t new_capacity = (capacity == 0) ? 16 : (capacity * 2);
          JoinedRow* tmp = realloc(result, new_capacity * sizeof(JoinedRow));
          if (tmp == NULL) {
            fprintf(stderr, "Не хватает памяти для результата\n");
            free(result);
            return NULL;
          }
          result = tmp;
          capacity = new_capacity;
        }

        result[size].id = left[i].id;
        strncpy(result[size].left, left[i].value, sizeof(result[size].left) - 1);
        result[size].left[sizeof(result[size].left) - 1] = '\0';
        strncpy(result[size].right, right[j].value, sizeof(result[size].right) - 1);
        result[size].right[sizeof(result[size].right) - 1] = '\0';
        size++;
      }
    }
  }

  *out_count = size;
  return result;
}

double seconds_between(const struct timespec* start, const struct timespec* end) {
  time_t sec = end->tv_sec - start->tv_sec;
  long nsec = end->tv_nsec - start->tv_nsec;
  if (nsec < 0) {
    sec -= 1;
    nsec += 1000000000L;
  }
  return (double)sec + (double)nsec / 1e9;
}
