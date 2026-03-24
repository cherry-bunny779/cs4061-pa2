CC = gcc
CFLAGS = -Wall -Werror -g
LDFLAGS = -lzstd

# List all your source files here
SRCS = main.c crawler.c storage.c stream.c restore.c

.PHONY: all clean

all: mgit

# Compile all source files directly into the binary
mgit: $(SRCS) mgit.h
	$(CC) $(CFLAGS) -o mgit $(SRCS) $(LDFLAGS)

clean:
	rm -f mgit
	rm -rf .mgit
