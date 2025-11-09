#pragma once

#include <stddef.h>
#include <time.h>

typedef struct {
  long long id;
  char value[9];
} Row;

typedef struct {
  long long id;
  char left[9];
  char right[9];
} JoinedRow;

Row* load_table(const char* path, size_t* out_count);
int save_result(const char* path, const JoinedRow* rows, size_t count);
JoinedRow* nested_loop_join(const Row* left,size_t left_count,const Row* right,size_t right_count,size_t* out_count);
double seconds_between(const struct timespec* start,const struct timespec* end);
