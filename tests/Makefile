CFLAGS = -g -Wall -Wextra -Wconversion
CPPFLAGS = -I../include
LDFLAGS = -L../target/debug
LDLIBS = -lleech2

.PHONY: all clean

all: test_c_ffi

test_c_ffi: test_c_ffi.o
	$(CC) $^ -o $@ $(LDFLAGS) $(LDLIBS) -Wl,-rpath,'$$ORIGIN/../target/debug'

test_c_ffi.o: test_c_ffi.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -f test_c_ffi.o
	rm -f test_c_ffi
