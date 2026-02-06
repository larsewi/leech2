CFLAGS = -g -Wall -Wextra -Wconversion
CPPFLAGS = -Iinclude
LDFLAGS = -Ltarget/debug
LDLIBS = -lleech2

.PHONY: all check diff commit

all: tests/.workdir/config.toml tests/leech2

tests/.workdir:
	mkdir -p tests/.workdir

tests/.workdir/config.toml: tests/.workdir tests/config.toml
	cp tests/config.toml tests/.workdir/

tests/leech2: tests/main.o
	$(CC) $^ -o $@ $(LDFLAGS) $(LDLIBS) -Wl,-rpath,'$$ORIGIN/../target/debug'

tests/main.o: tests/main.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

init: tests/.workdir/config.toml
	cp tests/foo.csv tests/.workdir

commit: tests/leech2
	./tests/leech2 tests/.workdir commit

diff: tests/leech2
	./tests/leech2 tests/.workdir diff $(ARGS)

clean:
	rm -f tests/main.o
	rm -f tests/leech2
	rm -rf tests/.workdir
