#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdbool.h>

#include "relaxed_stack.h"
#include "utils.h"

#include "mpigclock.h"

enum {
    NPUSH_WARMUP = 10,
    NRANDOPER      = NPUSH_WARMUP / 2,
    NRUNS          = 1
};

int myrank = 0, nproc = 0;

double time = 0;

bool ISDBG = false;

// warm_up: Push sufficient number of elements for warm up.
void warm_up(Buf_t *buf, int npush_warm_up_per_proc)
{
    for (int i = 0; i < npush_warm_up_per_proc; i++) {
        Val_t val = (myrank + 1) * 10 + i;
        buf_push(val, buf);
    }
}

// warm_up_proc: All elements will be pushed only to one process.
// Not relaxed stack warm up.
void warm_up_proc(Buf_t *buf, int npush_warm_up_per_proc)
{
    for (int i = 0; i < npush_warm_up_per_proc; i++) {
        Elem_t elem;
        elem.val = (myrank + 1) * 10 + i;

        buf_push_proc(elem, buf, myrank);
    }
}

// test_randopers: Test with random operations (push or pop)
// for the whole buffer.
void test_randopers(Buf_t *buf, int nrandoper_per_proc)
{
    for (int i = 0; i < nrandoper_per_proc; i++) {
        Val_t val = 0;

        if (get_rand(2) == 0) {
            buf_push(val, buf);
        } else {
            buf_pop(&val, buf);
        }
    }
}

// test_randopers_proc: Test with random operations (push or pop)
// for the specified process.
void test_randopers_proc(Buf_t *buf, int nrandoper_per_proc)
{
    if (myrank != 0) {
        for (int i = 0; i < nrandoper_per_proc; i++) {
            Elem_t elem;
            elem.val = 0;
            int remote_rank = 0;

            if (get_rand(2) == 0) {
                buf_push_proc(elem, buf, remote_rank);
            } else {
                buf_pop_proc(&elem, buf, remote_rank);
            }
        }
    }
}

// test_push_pop_debug: Test push and pop operations
// for the whole buffer
void test_push_pop_debug(Buf_t *buf, MPI_Comm comm)
{
    int rc;

    for (int i = 0; i < 3; i++) {
        Val_t val = (myrank + 1) * 10 + i;
        rc = buf_push(val, buf);

        if (rc == CODE_SUCCESS)
            printf("val %d inserted %d by proc. %d\n", val, i, myrank);
    }

    MPI_Barrier(comm);
    usleep(myrank * 1000);

    buf_print(buf, "PUSH");
    
    MPI_Barrier(comm);
    usleep(1000);

    for (int i = 0; i < 3; i++) {
        //printf("%d \t iter %d\n", myrank, i);
        Val_t val = 0;
        buf_pop(&val, buf);
    }

    MPI_Barrier(comm);
    usleep(myrank * 1000);
    buf_print(buf, "POP");
    MPI_Barrier(comm);
    usleep(1000);
}

// test_push_pop_proc: Test push and pop operations for specific
// processes (not distributed buffer)
void test_push_pop_proc(Buf_t *buf, MPI_Comm comm)
{
    int remote_rank = 0;
    buf_print(buf, "before");
    Elem_t elem;

    for (int i = 0; i < 25; i++) {
        elem.val = (myrank + 1) * 10 + i;
        buf_push_proc(elem, buf, remote_rank);

        MPI_Barrier(comm);
        //buf_print(buf, "PUSH");
    }

    for (int i = 0; i < 25; i++) {
        buf_pop_proc(&elem, buf, remote_rank);

        MPI_Barrier(comm);
        //printf("%d \t elem = %d\n", myrank, elem.val);
        //buf_print(buf, "POP");
    }
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    time = MPI_Wtime(); // *** DEBUG ***

    Buf_t *buf;

    int rc = buf_init(&buf, BUFFER_STARTSIZE, MPI_COMM_WORLD);

    if (rc != CODE_SUCCESS) {
        error_msg("buf_init() failed", 0);
        MPI_Abort(MPI_COMM_WORLD, CODE_ERROR);
        return CODE_ERROR;
    }

    int npush_warm_up_per_proc = NPUSH_WARMUP / nproc;
    assert(npush_warm_up_per_proc > 1);

    int nrandoper_per_proc = NRANDOPER / nproc;
    assert(nrandoper_per_proc > 1);

    MPI_Barrier(MPI_COMM_WORLD);

    // Relaxed stack warm up
    warm_up(buf, npush_warm_up_per_proc);

    // Not relaxed stack warmup
    //warm_up_proc(buf, npush_warm_up_per_proc);

    //buf_print(buf, "before");

    double telapsed_sum = 0.0;

    for (int i = 0; i < NRUNS; i++) {
        MPI_Barrier(MPI_COMM_WORLD);

        double tbegin = MPI_Wtime();

        // Test not relaxed stack
        //test_push_pop_proc(buf, MPI_COMM_WORLD);
        
        // Test relaxed stack
        //test_push_pop_debug(buf, MPI_COMM_WORLD);

        test_randopers(buf, nrandoper_per_proc);
        //test_randopers_proc(buf, nrandoper_per_proc);

        MPI_Barrier(MPI_COMM_WORLD);

        double tend = MPI_Wtime();

        if (myrank == 0) {
            // printf("Elapsed time (run %d): \t %lf\n", i, tend - tbegin);
            telapsed_sum += tend - tbegin;
        }
    }

    if (myrank == 0) {
        double telapsed_avg = telapsed_sum / NRUNS;
        double throughput_avg = NRANDOPER / telapsed_avg;

        printf("Number of processes: \t\t %d\n", nproc);
        printf("Number of warm up operations: \t %d\n", NPUSH_WARMUP);
        printf("Number of random operations: \t %d\n", NRANDOPER);
        printf("Number of runs: \t\t %d\n", NRUNS);
        printf("Elapsed time: \t\t %lf\n", telapsed_avg);
        printf("Throughput: \t\t %lf\n", throughput_avg);
    }

    //buf_print(buf, "after");

    buf_free(buf);
    
    MPI_Finalize();

    return 0;    
}