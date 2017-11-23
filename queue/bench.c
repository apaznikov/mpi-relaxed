/*
 * bench.c: Relaxed distributed queue implementation on MPI
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <unistd.h>

#include "relaxed_queue.h"
#include "utils.h"

enum {
    /* NINSERT_WARMUP = 150000, */
    NINSERT_WARMUP = 1000,
    /* NRANDOPER      = NINSERT_WARMUP / 2, */
    NRANDOPER      = 500,
    /* NRANDOPER      = 0, */
    NRUNS          = 1
};

int myrank = 0, nproc = 0;

bool ISDBG = false;

/* warm_up: Insert sufficient number of elements for warm up. */
void warm_up(circbuf_t *circbuf, int ninsert_warmup_per_proc)
{
    for (int i = 0; i < ninsert_warmup_per_proc; i++) {
        val_t val = (myrank + 1) * 10 + i;
        circbuf_insert(val, circbuf);
    }
}

/* test_randopers: Test with random operations (insert or remove)
 * for the whole circbuf */
void test_randopers(circbuf_t *circbuf, int nrandoper_per_proc)
{
    for (int i = 0; i < nrandoper_per_proc; i++) {
        val_t val = 0;
        if (get_rand(2) == 0) {
            circbuf_insert(val, circbuf);
        } else {
            circbuf_remove(&val, circbuf);
        }
    }
}

/* test_insert_remove: Test insert and remove operations 
 * for the whole circbuf */
void test_insert_remove_debug(circbuf_t *circbuf, MPI_Comm comm)
{
    for (int i = 0; i < 25; i++) {
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

    for (int i = 0; i < 25; i++) {
        printf("%d \t iter %d\n", myrank, i);
        val_t val = 0;
        circbuf_remove(&val, circbuf);

        /* MPI_Barrier(comm); #<{(| DEBUG |)}># */
        /* usleep(myrank * 1000); */
        /* printf("%d \t tail = %d, val = %d\n",  */
        /*         myrank, circbuf->state.head, val); */
        /* circbuf_print(circbuf, "REMOVE"); */
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

    for (int i = 0; i < 11; i++) {
        elem.val = (myrank + 1) * 10 + i;
        circbuf_insert_proc(elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
        circbuf_print(circbuf, "INSERT");
    }

    for (int i = 0; i < 11; i++) {
        circbuf_remove_proc(&elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t tail = %d, elem = %d\n", myrank, circbuf->state.tail, */
        /*         elem->val); */
        printf("%d \t elem = %d\n", myrank, elem.val);
        circbuf_print(circbuf, "REMOVE");
    }

    for (int i = 0; i < 5; i++) {
        elem.val = (myrank + 1) * 100 + i;
        circbuf_insert_proc(elem, circbuf, remote_rank);
        
        MPI_Barrier(comm); /* DEBUG */
        /* printf("%d \t head = %d\n", myrank, circbuf->state.head); */
        circbuf_print(circbuf, "INSERT");
    }
}

#include "mpigclock.h"

int main(int argc, char *argv[]) 
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    circbuf_t *circbuf;

    int rc = circbuf_init(&circbuf, CIRCBUF_STARTSIZE, MPI_COMM_WORLD);

    if (rc != CODE_SUCCESS) {
        error_msg("init_circbuf() failed", 0);
        goto error_lbl;
    }
    
    /* test_insert_remove_debug(circbuf, MPI_COMM_WORLD); */
    /* MPI_Finalize(); */
    /* return 0; */

    int ninsert_warmup_per_proc = NINSERT_WARMUP / nproc;
    assert(ninsert_warmup_per_proc > 1);

    int nrandoper_per_proc = NRANDOPER / nproc;
    assert(nrandoper_per_proc > 1);

    MPI_Barrier(MPI_COMM_WORLD);

    warm_up(circbuf, ninsert_warmup_per_proc);

    double telapsed_sum = 0.0;

    for (int i = 0; i < NRUNS; i++) {
        MPI_Barrier(MPI_COMM_WORLD);

        double tbegin = MPI_Wtime();

        /* test_insert_remove_proc(circbuf, MPI_COMM_WORLD); */
        test_randopers(circbuf, nrandoper_per_proc);
        /* test_insert_remove_debug(circbuf, MPI_COMM_WORLD); */

        MPI_Barrier(MPI_COMM_WORLD);

        double tend = MPI_Wtime();

        if (myrank == 0) {
            /* printf("Elapsed time (run %d): \t %lf\n", i, tend - tbegin); */
            telapsed_sum += tend - tbegin;
        }
    }

    if (myrank == 0) {
        double telapsed_avg = telapsed_sum / NRUNS;
        double throughput_avg = NRANDOPER / telapsed_avg;

        printf("Numer of processes: \t\t %d\n", nproc);
        printf("Numer of warm up operations: \t %d\n", NINSERT_WARMUP);
        printf("Numer of random operations: \t %d\n", NRANDOPER);
        printf("Numer of runs: \t\t %d\n", NRUNS);
        printf("Elapsed time: \t\t %lf\n", telapsed_avg);
        printf("Throughput: \t\t %lf\n", throughput_avg);
    }

    /* circbuf_print(circbuf, "after"); */

    circbuf_free(circbuf);

    MPI_Finalize();

    return 0;

error_lbl:
    MPI_Abort(MPI_COMM_WORLD, CODE_ERROR); 
}
