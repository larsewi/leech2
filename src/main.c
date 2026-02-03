#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

extern int add(int left, int right);

int main() {
  int a = 2, b = 2;
  int c = add(a, b);
  printf("%d + %d = %d\n", a, b, c);
  return EXIT_SUCCESS;
}
