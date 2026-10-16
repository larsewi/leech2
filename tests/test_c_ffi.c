/* Smoke test for the C FFI: exercises every public API function in leech2.h
 * to verify that the shared library links, runs, and returns success. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/* Per-row data for the callback-backed `events` table. */
typedef struct {
  double id;
  const char *event;
} event_row_t;

static const event_row_t events_rows[] = {
    {10.0, "login"},
    {11.0, "logout"},
};
static const size_t events_count = sizeof(events_rows) / sizeof(events_rows[0]);

/* Events supplied for the `alerts` event stream. */
static const char *const alerts_rows[] = {"disk full", "out of memory"};
static const size_t alerts_count = sizeof(alerts_rows) / sizeof(alerts_rows[0]);

typedef struct {
  int events_begin_count;
  int events_end_count;
  int alerts_begin_count;
  int alerts_end_count;
  int other_table_calls;
  int read_cell_success_count;
  int destroy_cell_count;
} cb_state_t;

static int test_table_begin(const char *table, void *usr_data) {
  cb_state_t *s = (cb_state_t *)usr_data;
  if (strcmp(table, "events") == 0) {
    s->events_begin_count++;
  } else if (strcmp(table, "alerts") == 0) {
    s->alerts_begin_count++;
  } else {
    s->other_table_calls++;
  }
  return LCH_SUCCESS;
}

static int test_table_end(const char *table, void *usr_data) {
  cb_state_t *s = (cb_state_t *)usr_data;
  if (strcmp(table, "events") == 0) {
    s->events_end_count++;
  } else if (strcmp(table, "alerts") == 0) {
    s->alerts_end_count++;
  } else {
    s->other_table_calls++;
  }
  return LCH_SUCCESS;
}

static int test_read_cell(const char *table, size_t row, size_t col,
                          const char *field_name, lch_cell_t *out_cell,
                          void *usr_data) {
  cb_state_t *s = (cb_state_t *)usr_data;
  (void)col;
  if (strcmp(table, "alerts") == 0) {
    if (row >= alerts_count) {
      return LCH_END_OF_TABLE;
    }
    if (strcmp(field_name, "message") != 0) {
      fprintf(stderr, "unexpected field '%s' for event stream '%s'\n",
              field_name, table);
      return LCH_FAILURE;
    }
    /* Static text: test_destroy_cell must not free it, so hand over a copy
     * like the `events` table does. */
    char *owned = strdup(alerts_rows[row]);
    if (owned == NULL) {
      return LCH_FAILURE;
    }
    out_cell->kind = LCH_VALUE_TEXT;
    out_cell->text = owned;
    s->read_cell_success_count++;
    return LCH_SUCCESS;
  }
  if (strcmp(table, "events") != 0) {
    s->other_table_calls++;
    return LCH_FAILURE;
  }
  if (row >= events_count) {
    return LCH_END_OF_TABLE;
  }
  const event_row_t *r = &events_rows[row];
  if (strcmp(field_name, "id") == 0) {
    out_cell->kind = LCH_VALUE_NUMBER;
    out_cell->number = r->id;
    s->read_cell_success_count++;
    return LCH_SUCCESS;
  }
  if (strcmp(field_name, "event") == 0) {
    /* Hand leech2 a freshly allocated copy; test_destroy_cell reclaims it
     * after leech2 has copied the text into its own storage. */
    char *owned = strdup(r->event);
    if (owned == NULL) {
      return LCH_FAILURE;
    }
    out_cell->kind = LCH_VALUE_TEXT;
    out_cell->text = owned;
    s->read_cell_success_count++;
    return LCH_SUCCESS;
  }
  fprintf(stderr, "unexpected field '%s' for table '%s'\n", field_name, table);
  return LCH_FAILURE;
}

/* Releases memory allocated for a cell in test_read_cell. Fires once for each
 * successful read_cell, for every kind; only TEXT cells own an allocation. */
static void test_destroy_cell(lch_cell_t *cell, void *usr_data) {
  cb_state_t *s = (cb_state_t *)usr_data;
  s->destroy_cell_count++;
  if (cell->kind == LCH_VALUE_TEXT) {
    free((void *)cell->text);
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <work_dir>\n", argv[0]);
    return EXIT_FAILURE;
  }
  const char *const work_dir = argv[1];

  log_state_t log_state = {0};
  lch_log_init(log_callback, &log_state);

  const char *version = lch_version();
  if (version == NULL || version[0] == '\0') {
    fprintf(stderr, "lch_version returned an empty string\n");
    return EXIT_FAILURE;
  }
  printf("leech2 version: %s\n", version);

  lch_config_t *cfg = lch_init(work_dir);
  if (cfg == NULL) {
    fprintf(stderr, "lch_init failed\n");
    return EXIT_FAILURE;
  }

  cb_state_t cb_state = {0};
  lch_callbacks_t callbacks = {
      .table_begin = test_table_begin,
      .read_cell = test_read_cell,
      .destroy_cell = test_destroy_cell,
      .table_end = test_table_end,
      .usr_data = &cb_state,
  };

  int ret = lch_block_create(cfg, &callbacks);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_block_create failed\n");
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  /* Callback-backed table fired begin once and end once with success.
   * CSV-backed table never reached the callback hooks. */
  if (cb_state.events_begin_count != 1) {
    fprintf(stderr, "expected 1 events begin, got %d\n",
            cb_state.events_begin_count);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  if (cb_state.events_end_count != 1) {
    fprintf(stderr, "expected 1 events end, got %d\n",
            cb_state.events_end_count);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  /* The event stream went through the same begin/end lifecycle. */
  if (cb_state.alerts_begin_count != 1 || cb_state.alerts_end_count != 1) {
    fprintf(stderr, "expected 1 alerts begin and end, got %d and %d\n",
            cb_state.alerts_begin_count, cb_state.alerts_end_count);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  if (cb_state.other_table_calls != 0) {
    fprintf(stderr,
            "callback hooks fired for a non-callback-backed table (%d calls)\n",
            cb_state.other_table_calls);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  /* destroy_cell fires exactly once for every successful read_cell, so each
   * strdup'd text cell is reclaimed exactly once. */
  if (cb_state.read_cell_success_count == 0) {
    fprintf(stderr, "expected at least one successful read_cell\n");
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  if (cb_state.destroy_cell_count != cb_state.read_cell_success_count) {
    fprintf(stderr, "expected %d destroy_cell calls, got %d\n",
            cb_state.read_cell_success_count, cb_state.destroy_cell_count);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  lch_buffer_t patch = {0};
  ret = lch_patch_create(cfg, NULL, &patch);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_create failed\n");
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  char *hash = NULL;
  ret = lch_patch_hash(&patch, &hash);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_hash failed\n");
    lch_buffer_free(&patch);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  if (hash == NULL || strlen(hash) != 40) {
    fprintf(stderr, "lch_patch_hash: expected 40-char hash, got '%s'\n",
            hash ? hash : "(null)");
    lch_string_free(hash);
    lch_buffer_free(&patch);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }
  printf("patch head: %s\n", hash);
  lch_string_free(hash);

  const lch_field_t injected[] = {
      {.name = "hostkey", .value = {.kind = LCH_VALUE_TEXT, .text = "abc123"}},
  };
  const size_t num_injected = sizeof(injected) / sizeof(injected[0]);

  char *sql = NULL;
  ret = lch_patch_to_sql(cfg, &patch, injected, num_injected, &sql);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_to_sql failed\n");
    lch_buffer_free(&patch);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }


  ret = lch_patch_applied(cfg, &patch);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_applied failed\n");
    lch_buffer_free(&patch);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  ret = lch_patch_failed(cfg);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_failed failed\n");
    lch_buffer_free(&patch);
    lch_deinit(cfg);
    return EXIT_FAILURE;
  }

  lch_buffer_free(&patch);
  lch_string_free(sql);
  lch_deinit(cfg);

  if (log_state.count == 0) {
    fprintf(stderr, "No log messages received\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
