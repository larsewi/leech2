/* Smoke test for the C FFI: exercises every public API function in leech2.h
 * to verify that the shared library links, runs, and returns success. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <leech2.h>

typedef struct {
  int count;
} log_state_t;

static void log_callback(lch_log_level_t level, const char *msg,
                         void *usr_data) {
  switch (level) {
  case LCH_LOG_ERROR:
    fprintf(stderr, "ERROR: %s\n", msg);
    break;
  case LCH_LOG_WARN:
    printf("WARN: %s\n", msg);
    break;
  case LCH_LOG_INFO:
    printf("INFO: %s\n", msg);
    break;
  case LCH_LOG_DEBUG:
    printf("DEBUG: %s\n", msg);
    break;
  case LCH_LOG_TRACE:
    printf("TRACE: %s\n", msg);
    break;
  }
  log_state_t *state = (log_state_t *)usr_data;
  state->count++;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <work_dir>\n", argv[0]);
    return EXIT_FAILURE;
  }
  const char *const work_dir = argv[1];

  log_state_t log_state = {0};
  lch_log_init(log_callback, &log_state);

  lch_config_t *cfg = lch_init(work_dir);
  if (cfg == NULL) {
    fprintf(stderr, "lch_init failed\n");
    return EXIT_FAILURE;
  }

  int ret = lch_block_create(cfg);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_block_create failed\n");
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  uint8_t *buf = NULL;
  size_t len = 0;
  ret = lch_patch_create(cfg, NULL, &buf, &len);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_create failed\n");
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  char *sql = NULL;
  ret = lch_patch_to_sql(cfg, buf, len, &sql);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_to_sql failed\n");
    lch_patch_free(buf, len);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  ret = lch_patch_applied(cfg, buf, len);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_applied failed\n");
    lch_patch_free(buf, len);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  ret = lch_patch_failed(cfg);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_failed failed\n");
    lch_patch_free(buf, len);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  lch_patch_free(buf, len);
  lch_sql_free(sql);
  lch_deinit(cfg);

  if (log_state.count == 0) {
    fprintf(stderr, "No log messages received\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
