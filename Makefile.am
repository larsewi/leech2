bin_PROGRAMS = helloworld
helloworld_SOURCES = src/main.c
helloworld_LDADD = target/debug/libimproved_system.a -lpthread -ldl -lm

target/debug/libimproved_system.a:
	cargo build
