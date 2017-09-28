/*
 * bench.c: Relaxed distributed queue implementation on MPI
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "relaxed_queue.h"

enum {
    NELEM = 100
};

int myrank = 0, nproc = 0;

void get_attr() {
    void *v;
    int vval;
    int flag;

    MPI_Attr_get(MPI_COMM_WORLD, MPI_WTIME_IS_GLOBAL, &v, &flag);
    if (flag) {
        vval = *(int*)v;
        printf("Value of \"MPI_WTIME_IS_GLOBAL\" is %d\n", vval);
    } else {
        printf("Error: cannot read the value of \"MPI_WTIME_IS_GLOBAL\"\n");
    }
}

/* test_insert_remove: Test insert and remove operations 
 * for the whole circbuf */
void test_insert_remove(circbuf_t *circbuf, MPI_Comm comm)
{
    int i;
    for (i = 0; i < 5; i++) {
        val_t val = (myrank + 1) * 10 + i;
        circbuf_insert(val, circbuf);

        /* MPI_Barrier(comm); #<{(| DEBUG |)}># */
        /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
        /* circbuf_print(circbuf, "INSERT"); */
        /* MPI_Barrier(comm); #<{(| DEBUG |)}># */
    }

    MPI_Barrier(comm); /* DEBUG */
    usleep(myrank * 1000);
    circbuf_print(circbuf, "INSERT");
    MPI_Barrier(comm); /* DEBUG */
    usleep(1000);

    if (myrank == 0) {
    for (i = 0; i < 1; i++) {
        val_t val = 0;
        circbuf_remove(&val, circbuf);

        /* MPI_Barrier(comm); #<{(| DEBUG |)}># */
        usleep(myrank * 1000);
        /* printf("%d \t tail = %d, val = %d\n",  */
        /*         myrank, circbuf->state.head, val); */
        circbuf_print(circbuf, "REMOVE");
    }
    }

    MPI_Barrier(comm); /* DEBUG */
    usleep(myrank * 1000);
    circbuf_print(circbuf, "REMOVE");
    MPI_Barrier(comm); /* DEBUG */
    usleep(1000);
}

/* test_insert_remove_proc: Test insert and remove operations for specific
 * processes (not distirbuted circbuf) */
void test_insert_remove_proc(circbuf_t *circbuf, MPI_Comm comm)
{
    int remote_rank = (myrank + 1) % nproc;
    circbuf_print(circbuf, "before");
    elem_t elem;

    int i;
    for (i = 0; i < 11; i++) {
        elem.val = (myrank + 1) * 10 + i;
        circbuf_insert_proc(elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
        circbuf_print(circbuf, "INSERT");
    }

    for (i = 0; i < 11; i++) {
        circbuf_remove_proc(&elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t tail = %d, elem = %d\n", myrank, circbuf->state.tail, */
        /*         elem->val); */
        printf("%d \t elem = %d\n", myrank, elem.val);
        circbuf_print(circbuf, "REMOVE");
    }

    for (i = 0; i < 5; i++) {
        elem.val = (myrank + 1) * 100 + i;
        circbuf_insert_proc(elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
        circbuf_print(circbuf, "INSERT");
    }
}

int main(int argc, char *argv[]) 
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    get_attr();

    circbuf_t *circbuf;

    int rc = circbuf_init(&circbuf, CIRCBUF_STARTSIZE, MPI_COMM_WORLD);

    if (rc != CODE_SUCCESS) {
        error_msg("init_circbuf() failed", 0);
        goto error_lbl;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* test_insert_remove_proc(circbuf, MPI_COMM_WORLD); */
    test_insert_remove(circbuf, MPI_COMM_WORLD);

    /* printf("%d \t before \t lock = %d\n", myrank, circbuf->lock.state); */

    MPI_Barrier(MPI_COMM_WORLD);

    /* circbuf_print(circbuf, "after"); */

    /* printf("%d \t after all: \t lock = %d\n", myrank, circbuf->lock.state); */

    circbuf_free(circbuf);

    MPI_Finalize();

    return 0;

error_lbl:
    MPI_Abort(MPI_COMM_WORLD, CODE_ERROR); 
}
