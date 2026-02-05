CFLAGS = -g -Wall -Wextra -Wconversion
CPPFLAGS = -Iinclude
LDFLAGS = -Ltarget/debug
LDLIBS = -limproved_system

.PHONY: all check

all:
	cargo build

check: tests/isys
	mkdir -p tests/.workdir
	cp tests/config.toml tests/.workdir/
	RUST_LOG=debug ./tests/isys tests/.workdir commit

tests/isys: tests/main.o
	$(CC) $^ -o $@ $(LDFLAGS) $(LDLIBS) -Wl,-rpath,'$$ORIGIN/../target/debug'

tests/main.o: tests/main.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -f tests/main.o
	rm -f tests/isys
	rm -rf tests/.workdir
