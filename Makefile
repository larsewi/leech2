CFLAGS = -g -Wall -Wextra -Wconversion
CPPFLAGS = -Iinclude
LDFLAGS = -Ltarget/debug
LDLIBS = -limproved_system

.PHONY: all check

all:
	cargo build

check: tests/prog
	mkdir -p tests/.workdir
	cp tests/config.toml tests/.workdir/
	RUST_LOG=info ./tests/prog tests/.workdir commit

tests/prog: tests/main.o
	$(CC) $^ -o $@ $(LDFLAGS) $(LDLIBS) -Wl,-rpath,'$$ORIGIN/../target/debug'

tests/main.o: tests/main.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -f tests/main.o
	rm -f tests/prog
	rm -rf tests/.workdir
