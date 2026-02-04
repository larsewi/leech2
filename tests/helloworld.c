#include <stdint.h>
#include <stdlib.h>

#include <improved.h>

int main() {
  int ret = isys_init(".improved");
  if (ret != 0) {
    return EXIT_FAILURE;
  }

  ret = isys_commit();
  return (ret == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
