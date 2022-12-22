CC=gcc
#CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -fprofile-arcs -ftest-coverage -pthread
#CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -Ofast -pthread
CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -g3 -pthread

all: example

example: example.c phash.o

phash.o: phash.c phash.h

_phash.c: setup_ffi.py
	python3 setup_ffi.py

.PHONY:
clean:
	rm -f example *.o
