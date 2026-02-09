#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <leech2.h>

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Missing work directory argument\n");
    return EXIT_FAILURE;
  }
  const char *const work_dir = argv[1];
  const char *const command = argv[2];

  int ret = lch_init(work_dir);
  if (ret != 0) {
    fprintf(stderr, "Failed to initialize\n");
    return EXIT_FAILURE;
  }

  if (strcmp(command, "commit") == 0) {
    ret = lch_block_create();
    if (ret != 0) {
      fprintf(stderr, "Failed to create block\n");
      return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
  }

  if (strcmp(argv[2], "diff") == 0) {
    if (argc < 4) {
      fprintf(stderr, "Missing block argument\n");
      return EXIT_FAILURE;
    }
    const char *const block = argv[3];

    uint8_t *patch = NULL;
    size_t patch_len = 0;
    ret = lch_patch_create(block, &patch, &patch_len);
    if (ret != 0) {
      fprintf(stderr, "Failed to create patch\n");
      return EXIT_FAILURE;
    }
    fwrite(patch, 1, patch_len, stdout);
    lch_free(patch, patch_len);
    return EXIT_SUCCESS;
  }

  fprintf(stderr, "Bad command '%s'\n", command);
  return EXIT_FAILURE;
}
