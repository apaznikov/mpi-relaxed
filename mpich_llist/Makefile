LLIST=linked_list
LLIST_FOP=linked_list_fop
LLIST_LOCKALL=linked_list_lockall
LLIST_BENCH_LOCK_ALL=linked_list_bench_lock_all
LLIST_BENCH_LOCK_EXCL=linked_list_bench_lock_excl
LLIST_BENCH_LOCK_SHR=linked_list_bench_lock_shr

CC=mpicc
CFLAGS=-Wall 

all: 
	$(CC) $(CFLAGS) src/$(LLIST).c -o bin/$(LLIST)
	$(CC) $(CFLAGS) src/$(LLIST_FOP).c -o bin/$(LLIST_FOP)
	$(CC) $(CFLAGS) src/$(LLIST_LOCKALL).c -o bin/$(LLIST_LOCKALL)
	$(CC) $(CFLAGS) src/$(LLIST_BENCH_LOCK_ALL).c -o bin/$(LLIST_BENCH_LOCK_ALL)
	$(CC) $(CFLAGS) src/$(LLIST_BENCH_LOCK_EXCL).c -o bin/$(LLIST_BENCH_LOCK_EXCL)
	$(CC) $(CFLAGS) src/$(LLIST_BENCH_LOCK_SHR).c -o bin/$(LLIST_BENCH_LOCK_SHR)

clean:
	rm -rf $(LLIST) $(LLIST_FOP) $(LLIST_LOCKALL) $(LLIST_BENCH_LOCK_ALL) \
		   $(LLIST_BENCH_LOCK_EXCL) $(LLIST_BENCH_LOCK_SHR)
