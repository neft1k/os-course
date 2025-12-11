#include "ema_join.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Row* load_table(const char* path, size_t* out_count) {
  FILE* file = fopen(path, "r");
  if (file == NULL) {
    fprintf(stderr, "Не удалось открыть %s: %s\n", path, strerror(errno));
    return NULL;
  }

  unsigned long long rows = 0;
  if (fscanf(file, "%llu", &rows) != 1) {
    fprintf(stderr, "Не удалось прочитать количество строк в %s\n", path);
    fclose(file);
    return NULL;
  }

  Row* data = calloc(rows, sizeof(Row));
  if (data == NULL) {
    fprintf(stderr, "Не хватает памяти для чтения %s\n", path);
    fclose(file);
    return NULL;
  }

  for (unsigned long long i = 0; i < rows; ++i) {
    long long id = 0;
    char word[16] = {0};
    if (fscanf(file, "%lld %15s", &id, word) != 2) {
      fprintf(stderr, "Строка %llu в %s некорректна\n", i + 1, path);
      free(data);
      fclose(file);
      return NULL;
    }
    data[i].id = id;
    strncpy(data[i].value, word, sizeof(data[i].value) - 1);
    data[i].value[sizeof(data[i].value) - 1] = '\0';
  }

  fclose(file);
  *out_count = (size_t)rows;
  return data;
}

int save_result(const char* path, const JoinedRow* rows, size_t count) {
  FILE* file = fopen(path, "w");
  if (file == NULL) {
    fprintf(stderr, "Не удалось открыть %s для записи: %s\n", path, strerror(errno));
    return -1;
  }

  fprintf(file, "%zu\n", count);
  for (size_t i = 0; i < count; ++i) {
    fprintf(file, "%lld %s %s\n", rows[i].id, rows[i].left, rows[i].right);
  }

  fclose(file);
  return 0;
}
