CFLAGS = -g -Wall -Wextra -Wconversion
CPPFLAGS = -Iinclude
LDFLAGS = -Ltarget/debug
LDLIBS = -limproved_system -lpthread -ldl

.PHONY: all check

all:
	cargo build

check: helloworld
	cargo test

helloworld: tests/helloworld.o
	$(CC) $^ -o $@ $(LDFLAGS) $(LDLIBS)

helloworld.o: tests/helloworld.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@
