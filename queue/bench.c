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

/* test_insert_remove: Test insert and remove operations 
 * for the whole circbuf */
void test_insert_remove(circbuf_t *circbuf)
{
    int i;
    for (i = 0; i < 11; i++) {
        val_t val = (myrank + 1) * 10 + i;
        circbuf_insert(val, circbuf);

        MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
        printf("%d \t head = %d\n", myrank, circbuf->state.head);
        circbuf_print(circbuf, "after INSERT");
        MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
    }
}

/* test_insert_remove_proc: Test insert and remove operations for specific
 * processes (not distirbuted circbuf) */
void test_insert_remove_proc(circbuf_t *circbuf)
{
    /* if (myrank == 1) { */
        int remote_rank = (myrank + 1) % nproc;
        circbuf_print(circbuf, "before");
        elem_t *elem = malloc(sizeof(elem_t));

        int i;
        for (i = 0; i < 11; i++) {
            elem->val = (myrank + 1) * 10 + i;
            circbuf_insert_proc(*elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
            circbuf_print(circbuf, "after insert");
        }

        for (i = 0; i < 11; i++) {
            circbuf_remove_proc(elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            /* printf("%d \t tail = %d, elem = %d\n", myrank, circbuf->state.tail, */
            /*         elem->val); */
            printf("%d \t elem = %d\n", myrank, elem->val);
            circbuf_print(circbuf, "after remove");
        }

        for (i = 0; i < 5; i++) {
            elem->val = (myrank + 1) * 100 + i;
            circbuf_insert_proc(*elem, circbuf, remote_rank);
            
            MPI_Barrier(MPI_COMM_WORLD); /* DEBUG */
            /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
            circbuf_print(circbuf, "after");
        }

        free(elem);
        
    /* } */
}

int main(int argc, char *argv[]) 
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    circbuf_t *circbuf;

    int rc = circbuf_init(&circbuf, CIRCBUF_STARTSIZE);

    if (rc != CODE_SUCCESS) {
        error_msg("init_circbuf() failed", 0);
        goto error_lbl;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* test_insert_remove_proc(circbuf); */
    test_insert_remove(circbuf);

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
