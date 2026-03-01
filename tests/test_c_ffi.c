#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <leech2.h>

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <work_dir>\n", argv[0]);
    return EXIT_FAILURE;
  }
  const char *const work_dir = argv[1];

  lch_config_t *config = lch_init(work_dir);
  if (config == NULL) {
    fprintf(stderr, "lch_init failed\n");
    return EXIT_FAILURE;
  }

  int ret = lch_block_create(config);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_block_create failed\n");
    lch_deinit(config);
    return EXIT_FAILURE;
  }

  uint8_t *buf = NULL;
  size_t len = 0;
  ret = lch_patch_create(config, NULL, &buf, &len);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_create failed\n");
    lch_deinit(config);
    return EXIT_FAILURE;
  }

  char *sql = NULL;
  ret = lch_patch_to_sql(config, buf, len, &sql);
  if (ret == LCH_FAILURE) {
    fprintf(stderr, "lch_patch_to_sql failed\n");
    lch_patch_applied(config, buf, len, 0);
    lch_deinit(config);
    return EXIT_FAILURE;
  }

  lch_patch_applied(config, buf, len, 1);

  if (sql != NULL) {
    lch_free_sql(sql);
  }

  lch_deinit(config);

  return EXIT_SUCCESS;
}
