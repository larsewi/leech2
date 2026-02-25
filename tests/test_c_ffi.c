#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <leech2.h>

#define GENESIS_HASH "0000000000000000000000000000000000000000"

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <work_dir>\n", argv[0]);
    return EXIT_FAILURE;
  }
  const char *const work_dir = argv[1];

  int ret = lch_init(work_dir);
  if (ret != 0) {
    fprintf(stderr, "lch_init failed\n");
    return EXIT_FAILURE;
  }

  ret = lch_block_create();
  if (ret != 0) {
    fprintf(stderr, "lch_block_create failed\n");
    return EXIT_FAILURE;
  }

  uint8_t *buf = NULL;
  size_t len = 0;
  ret = lch_patch_create(GENESIS_HASH, &buf, &len);
  if (ret != 0) {
    fprintf(stderr, "lch_patch_create failed\n");
    return EXIT_FAILURE;
  }

  char *sql = NULL;
  ret = lch_patch_to_sql(buf, len, &sql);
  if (ret != 0) {
    fprintf(stderr, "lch_patch_to_sql failed\n");
    lch_patch_applied(buf, len, 0);
    return EXIT_FAILURE;
  }

  lch_patch_applied(buf, len, 1);

  if (sql != NULL) {
    lch_free_sql(sql);
  }

  return EXIT_SUCCESS;
}
