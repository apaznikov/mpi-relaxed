PROG=mpisync.o
PROG_OBJ=hpctimer.o mpigclock.o

CC=mpicc
CFLAGS=-Wall 

all: $(PROG)

$(PROG): $(PROG_OBJ)
	ld -r $? -o $@

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ 

clean:
	rm -rf $(PROG) $(PROG_OBJ) 
