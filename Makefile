CC=gcc

DEPS = dict.h crc.h
BUILD = release
CFLAGS_release = 
CFLAGS_debug = -g -O0
CFLAGS = ${CFLAGS_${BUILD}}

CFLAGS += -Wall -Wextra
CFLAGS += -DDICT_BENCHMARK_MAIN

# EXAMPLE: 
#   $ make clean && make BUILD=debug

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

dict_bench: dict.o crc.o
	$(CC) -o $@ $^

.PHONY: bench
bench: dict_bench
	./dict_bench

.PHONY: pretty
pretty:
	clang-format -i *.c *.h

.PHONY: anyway
anyway: clean bench

.PHONY: clean
clean:
	rm -f *.o 