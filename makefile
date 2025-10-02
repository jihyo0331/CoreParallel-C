CC      := gcc
CFLAGS  := -O3 -fno-omit-frame-pointer -Wall -Wextra -Iinclude
LDLIBS  := -lpthread -lbz2

all: demo

demo: src/parallel.c demo.c include/parallel.h
	$(CC) $(CFLAGS) src/parallel.c demo.c -o $@ $(LDLIBS)

clean:
	rm -f demo
