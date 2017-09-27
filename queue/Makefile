PROG=bench
PROG_OBJ=$(PROG).o relaxed_queue.o

CC=mpicc
CFLAGS=-Wall 

all: $(PROG)

$(PROG): $(PROG_OBJ) $(RELAXED_QUEUE)
	$(CC) $(CFLAGS) $^ -o $@ 

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ 

clean:
	rm -rf $(PROG) $(PROG_OBJ) 
