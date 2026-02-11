#include <stddef.h>
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
    char path[4096];
    snprintf(path, sizeof(path), "%s/PATCH", work_dir);

    FILE *f = fopen(path, "wb");
    if (f == NULL) {
      fprintf(stderr, "Failed to open '%s' for writing\n", path);
      lch_free_buf(patch, patch_len);
      return EXIT_FAILURE;
    }
    if (fwrite(patch, 1, patch_len, f) != patch_len) {
      fprintf(stderr, "Failed to write to '%s'\n", path);
      fclose(f);
      lch_free_buf(patch, patch_len);
      return EXIT_FAILURE;
    }
    fclose(f);

    lch_free_buf(patch, patch_len);
    return EXIT_SUCCESS;
  }

  if (strcmp(command, "patch") == 0) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/PATCH", work_dir);

    FILE *f = fopen(path, "rb");
    if (f == NULL) {
      fprintf(stderr, "Failed to open '%s' for reading\n", path);
      return EXIT_FAILURE;
    }

    fseek(f, 0, SEEK_END);
    long pos = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (pos < 0) {
      fprintf(stderr, "Failed to determine size of file '%s'\n", path);
      fclose(f);
      return EXIT_FAILURE;
    }
    size_t len = (size_t)pos;

    uint8_t *patch = malloc(len);
    if (patch == NULL) {
      fprintf(stderr, "Failed to allocate memory\n");
      fclose(f);
      return EXIT_FAILURE;
    }

    if (fread(patch, 1, len, f) != len) {
      fprintf(stderr, "Failed to read from '%s'\n", path);
      free(patch);
      fclose(f);
      return EXIT_FAILURE;
    }
    fclose(f);

    char *sql = NULL;
    ret = lch_patch_to_sql(patch, len, &sql);
    free(patch);
    if (ret != 0) {
      fprintf(stderr, "Failed to convert patch to SQL\n");
      return EXIT_FAILURE;
    }

    if (sql) {
      printf("%s", sql);
      lch_free_str(sql);
    }
    return EXIT_SUCCESS;
  }

  fprintf(stderr, "Bad command '%s'\n", command);
  return EXIT_FAILURE;
}
