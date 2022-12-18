CC=gcc
#CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -fprofile-arcs -ftest-coverage -pthread
#CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -Ofast -pthread
CFLAGS= -latomic -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -g3 -pthread

#all: phash phash.so
all: example

example: phash.c phash.h example.c

.PHONY:
clean:
	rm -f example *.o
