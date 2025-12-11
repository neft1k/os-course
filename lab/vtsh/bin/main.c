#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#define MAX_INPUT 1024
#define MAX_ARGS 64

static char* skip_spaces(char* s) {
  while (*s == ' ' || *s == '\t')
    s++;
  return s;
}

int run_command(char *cmd) {
  char *args[MAX_ARGS];
  pid_t pid;
  int status;

  int argc = 0;
  char *token = strtok(cmd, " \t");
  while (token != NULL && argc < MAX_ARGS - 1) {
    args[argc++] = token;
    token = strtok(NULL, " \t");
  }
  args[argc] = NULL;

  if (argc == 0)
    return 0;

  if (strcmp(args[0], "exit") == 0)
    exit(0);

  struct timespec start = {0}, end = {0};
  clock_gettime(CLOCK_MONOTONIC, &start);

  pid = vfork();
  if (pid < 0)
    _exit(1);

  if (pid == 0) {
    execvp(args[0], args);
    const char msg[] = "Command not found\n";
    write(STDOUT_FILENO, msg, sizeof(msg) - 1);
    _exit(127);
  }

  if (waitpid(pid, &status, 0) < 0)
    return 1;

  clock_gettime(CLOCK_MONOTONIC, &end);
  time_t sec = end.tv_sec - start.tv_sec;
  long nsec = end.tv_nsec - start.tv_nsec;
  if (nsec < 0) {
    sec -= 1;
    nsec += 1000000000L;
  }
  double time_spent = (double)sec + (double)nsec / 1e9;
  fprintf(stderr, "time=%.6f\n", time_spent);

  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  if (WIFSIGNALED(status))
    return 128 + WTERMSIG(status);
  return 1;
}

int main() {
  char input[MAX_INPUT];
  setvbuf(stdin, NULL, _IONBF, 0);

  while (1) {
    if (fgets(input, sizeof(input), stdin) == NULL)
      break;

    input[strcspn(input, "\n")] = '\0';
    char *cmd = skip_spaces(input);
    if (*cmd == '\0')
      continue;

    int code = 0;

    while (cmd != NULL && *cmd != '\0') {
      char *and_ptr = strstr(cmd, "&&");

      if (and_ptr != NULL) {
        *and_ptr = '\0';
      }

      char *segment = skip_spaces(cmd);
      if (*segment == '\0') {
        fprintf(stderr, "syntax error: empty command\n");
        break;
      }

      code = run_command(segment);

      if (code != 0)
        break;

      if (and_ptr == NULL)
        break;

      cmd = skip_spaces(and_ptr + 2);
      if (*cmd == '\0') {
        fprintf(stderr, "syntax error: trailing &&\n");
        break;
      }
    }
  }

  return 0;
}
