# AssemblyDB - Pure AArch64 Assembly Embedded Database
# Build system for shared library + tests

AS      = as
LD      = ld
CC      = gcc
AR      = ar

ASFLAGS = -march=armv8-a+crc+crypto -I .
CFLAGS  = -Wall -Wextra -O2 -I include
LDFLAGS = -shared -soname libassemblydb.so -T linker.ld --gc-sections

# Find all assembly source files (exclude const.s which is include-only)
SRCS    = $(shell find src -name '*.s' ! -name 'const.s' | sort)
OBJS    = $(SRCS:.s=.o)

# Output
LIB     = libassemblydb.so
ALIB    = libassemblydb.a

.PHONY: all clean test bench

all: $(LIB) $(ALIB)

# Assemble each .s file to .o
%.o: %.s src/const.s
	$(AS) $(ASFLAGS) -o $@ $<

# Shared library
$(LIB): $(OBJS)
	$(LD) $(LDFLAGS) -o $@ $(OBJS)

# Static library
$(ALIB): $(OBJS)
	$(AR) rcs $@ $(OBJS)

# Test targets
test: $(ALIB) tests/test_core.c
	$(CC) $(CFLAGS) -o test_core tests/test_core.c -L. -l:libassemblydb.a -static
	./test_core

test_btree: $(ALIB) tests/test_btree.c
	$(CC) $(CFLAGS) -o test_btree tests/test_btree.c -L. -l:libassemblydb.a -static
	./test_btree

test_lsm: $(ALIB) tests/test_lsm.c
	$(CC) $(CFLAGS) -o test_lsm tests/test_lsm.c -L. -l:libassemblydb.a -static
	./test_lsm

test_integration: $(ALIB) tests/test_integration.c
	$(CC) $(CFLAGS) -o test_integration tests/test_integration.c -L. -l:libassemblydb.a -static
	./test_integration

test_mvcc: $(ALIB) tests/test_mvcc.c
	$(CC) $(CFLAGS) -o test_mvcc tests/test_mvcc.c -L. -l:libassemblydb.a -static
	./test_mvcc

test_crypto: $(ALIB) tests/test_crypto.c
	$(CC) $(CFLAGS) -o test_crypto tests/test_crypto.c -L. -l:libassemblydb.a -static
	./test_crypto

test_compress: $(ALIB) tests/test_compress.c
	$(CC) $(CFLAGS) -o test_compress tests/test_compress.c -L. -l:libassemblydb.a -static
	./test_compress

test_stress: $(ALIB) tests/test_stress.c
	$(CC) $(CFLAGS) -o test_stress tests/test_stress.c -L. -l:libassemblydb.a -static
	./test_stress

test_all: test test_btree test_lsm test_mvcc test_crypto test_compress test_integration

bench: $(ALIB) tests/bench.c
	$(CC) $(CFLAGS) -O3 -o bench tests/bench.c -L. -l:libassemblydb.a -static
	./bench

# Size report
size: $(LIB) $(ALIB)
	@echo "=== Shared library ==="
	@ls -la $(LIB)
	@size $(LIB)
	@echo ""
	@echo "=== Static library ==="
	@ls -la $(ALIB)
	@echo ""
	@echo "=== Object files ==="
	@size $(OBJS) | tail -1
	@echo ""
	@echo "=== Per-file sizes ==="
	@for f in $(OBJS); do printf "  %6d  %s\n" $$(stat -c%s $$f) $$f; done

clean:
	find src -name '*.o' -delete
	rm -f $(LIB) $(ALIB) test_core test_btree test_lsm test_mvcc test_crypto test_compress test_integration test_stress bench
