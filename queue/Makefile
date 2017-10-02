PROG=bench

MPISYNC_PATH=../mpisync
MPISYNC=$(MPISYNC_PATH)/mpisync.o

PROG_OBJ=$(PROG).o utils.o relaxed_queue.o $(MPISYNC)

INCLUDE_PATH=$(MPISYNC_PATH)

CC=mpicc
CFLAGS=-Wall -I$(INCLUDE_PATH)

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
