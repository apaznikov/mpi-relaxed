CC=mpicc
CFLAGS=-Wall
PROG=relaxed_queue
SRC=$(PROG).c

SRCDIR=.
BINDIR=.

all: compile

compile: $(SRCDIR)/$(SRC)
	$(CC) $(CFLAGS) $(SRCDIR)/$(SRC) -o $(BINDIR)/$(PROG)

clean:
	rm $(BINDIR)/$(PROG)
