PROG=bench

MPISYNC_PATH=../mpisync
MPISYNC=$(MPISYNC_PATH)/mpisync.o

PROG_OBJ=$(PROG).o relaxed_queue.o $(MPISYNC)

CC=mpicc
CFLAGS=-Wall 

all: $(PROG)

$(PROG): $(PROG_OBJ) $(RELAXED_QUEUE) $(MPIGCLOCK)
	$(CC) $(CFLAGS) $^ -o $@ 

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ 

$(MPISYNC):
	$(MAKE) -C $(MPISYNC_PATH)

clean:
	rm -rf $(PROG) $(PROG_OBJ) 
	$(MAKE) -C $(MPISYNC_PATH) clean
