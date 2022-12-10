CC=gcc
CFLAGS= -Wall -Wextra -Wpedantic -Werror -Wshadow -Wformat=2 -fno-common -g3 -pthread -lpcap 

all: phash 

phash: phash.c phash.h

.PHONY:
clean:
	rm -f ashnetd *.o
