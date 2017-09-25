/*
 * bench.c: Relaxed distributed queue implementation on MPI
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#include <stdio.h>
#include <stdlib.h>

#include "relaxed_queue.h"

enum {
    NELEM = 100
};

int myrank = 0, nproc = 0;

/* test_insert_remove_proc: Test insert and remove operations for specific
 * processes (not distirbuted circbuf) */
static void test_insert_remove_proc(circbuf_t *circbuf)
{
    /* if (myrank == 1) { */
        int remote_rank = (myrank + 1) % nproc;
        circbuf_print(circbuf, "before");
        elem_t *elem = malloc(sizeof(elem_t));

        int i;
        for (i = 0; i < 11; i++) {
            *elem = (myrank + 1) * 10 + i;
            circbuf_insert_proc(*elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            printf("%d \t head = %d\n", myrank, circbuf->state.head);
            circbuf_print(circbuf, "after");
        }

        for (i = 0; i < 10; i++) {
            circbuf_remove_proc(elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            printf("%d \t tail = %d\n", myrank, circbuf->state.tail);
            circbuf_print(circbuf, "after");
        }

        for (i = 0; i < 5; i++) {
            *elem = (myrank + 1) * 100 + i;
            circbuf_insert_proc(*elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            printf("%d \t head = %d\n", myrank, circbuf->state.head);
            circbuf_print(circbuf, "after");
        }

        free(elem);
        
    /* } */
}

int main(int argc, char *argv[]) 
{
    int rc;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    circbuf_t *circbuf;

    rc = circbuf_init(&circbuf, CIRCBUF_STARTSIZE);

    if (rc != CODE_SUCCESS) {
        error_msg("init_circbuf() failed", 0);
        goto error_lbl;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    test_insert_remove_proc(circbuf);

    printf("%d \t before \t lock = %d\n", myrank, circbuf->lock.state);

    MPI_Barrier(MPI_COMM_WORLD);

    /* circbuf_print(circbuf, "after"); */

    /* printf("%d \t after all: \t lock = %d\n", myrank, circbuf->lock.state); */

    /* int i; */
    /* elem_t elem = rank; */
    /* int remote_rank = 0; */
    /* for (i = 0; i < NELEM; i++) { */
    /*     circbuf_insert(circbuf, elem, disps[remote_rank], remote_rank); */
    /* } */
    
    circbuf_free(circbuf);

error_lbl:
    MPI_Finalize();

    return 0;
}
