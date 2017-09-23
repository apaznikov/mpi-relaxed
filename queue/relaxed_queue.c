#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <mpi.h>

int myrank = 0;

typedef int bool;
typedef int elem_t;

typedef struct {
    int head;                   /* Write pointer */
    int tail;                   /* Read pointer */
    int size;                   /* Max number of elements */
} circbuf_state_t;

/* Circular buffer. */
typedef struct {
    MPI_Aint basedisp;          /* Base address of circbuf */
    elem_t *buf;                /* Physical buffer */
    MPI_Aint datadisp;          /* Address of buf (data) */
    circbuf_state_t state;
    int lock;                   /* Spinlock variable */
    /* lock_t lock;                #<{(| Spinlock variable |)}># */
    MPI_Win win;
} circbuf_t;

/* Auxiliary buffers for lock. */
typedef struct {
    int state;
    int unlocked; 
    int locked;
    int result;
} lock_t;

/* Process-oblivious circular buffer info */
typedef struct {
    size_t lock_offset;
    size_t state_offset;
    size_t buf_offset;
    int lock_locked;
    int lock_unlocked;
    int lock_result;
} circbuf_info_t;

circbuf_info_t circbuf_info;

enum {
    NELEM             = 100,
    CIRCBUF_STARTSIZE = 10,
    CODE_ERROR        = 1,
    CODE_SUCCESS      = 0
};

/* error_msg: Print error message. */
static void error_msg(const char *msg)
{
    fprintf(stderr, "%s", msg);
    if (errno != 0)
        fprintf(stderr, ": %s", strerror(errno));
    fprintf(stderr, "\n");
}

/* circbuf_info_init: Initialize process-oblivious information */
static void circbuf_info_init(void)
{
    circbuf_info.state_offset = offsetof(circbuf_t, state);
    circbuf_info.lock_offset = offsetof(circbuf_t, lock);
    circbuf_info.buf_offset = offsetof(circbuf_t, buf);
    circbuf_info.lock_locked = 1;
    circbuf_info.lock_unlocked = 0;
}

/* circbuf_init: Init circular buffer with specified size. */
static int circbuf_init(circbuf_t **circbuf, int size)
{
    /* *circbuf = malloc(sizeof(circbuf_t)); */
    MPI_Alloc_mem(sizeof(circbuf_t), MPI_INFO_NULL, circbuf);
    if (*circbuf == NULL) {
        error_msg("malloc() failed for circbuf");
        return CODE_ERROR;
    }

    MPI_Get_address(*circbuf, &(*circbuf)->basedisp);

    const int size_bytes = sizeof(elem_t) * size;

    /* (*circbuf)->buf = malloc(size_bytes); */
    MPI_Alloc_mem(size_bytes, MPI_INFO_NULL, &(*circbuf)->buf);
    if ((*circbuf)->buf == NULL) {
        error_msg("malloc() failed for circbuf->buf");
        return CODE_ERROR;
    }

    memset((*circbuf)->buf, 0, size_bytes);

    MPI_Get_address((*circbuf)->buf, &(*circbuf)->datadisp);

    (*circbuf)->state.head = 0;
    (*circbuf)->state.tail = 0;
    (*circbuf)->state.size = size;

    (*circbuf)->lock = 0;

    circbuf_info_init();

    MPI_Win_create_dynamic(MPI_INFO_NULL, MPI_COMM_WORLD, &(*circbuf)->win);

    MPI_Win_attach((*circbuf)->win, *circbuf, sizeof(circbuf_t));
    MPI_Win_attach((*circbuf)->win, (*circbuf)->buf, size_bytes);

    return CODE_SUCCESS;
}

/* mutex_lock: */
static void mutex_lock(MPI_Win win, MPI_Aint basedisp, int rank)
{
    /* MPI_Win_lock_all(0, circbuf->win); */
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, win); 

    do {
        MPI_Compare_and_swap(&circbuf_info.lock_locked, 
                             &circbuf_info.lock_unlocked, 
                             &circbuf_info.lock_result, MPI_INT, rank,
                             MPI_Aint_add(basedisp, 
                                          circbuf_info.lock_offset),
                             win);

        MPI_Win_flush(rank, win);
    } while (circbuf_info.lock_result == 0); 
}

/* mutex_unlock: */
static void mutex_unlock(MPI_Win win, MPI_Aint basedisp, int rank)
{
    MPI_Compare_and_swap(&circbuf_info.lock_unlocked, 
                         &circbuf_info.lock_locked, 
                         &circbuf_info.lock_result, MPI_INT, rank,
                         MPI_Aint_add(basedisp, 
                                      circbuf_info.lock_offset),
                         win);

    MPI_Win_flush(rank, win);

    /* MPI_Win_unlock_all(circbuf->win); */
    MPI_Win_unlock(rank, win);
}

/* isempty: Check if buffer is empty */
/* static bool isempty(int head, int tail)  */
/* { */
/*     return head == tail; */
/* } */

/* isfull: Check if buffer is full */
static bool isfull(circbuf_state_t state) 
{
    return ((state.head + 1) % state.size) == state.tail;
}

/* get_circbuf_state: Get state of remote circbuf (head, tail, size) */
static void get_circbuf_state(MPI_Win win, MPI_Aint basedisp, 
                              int rank, circbuf_state_t *state)
{
    MPI_Get(state, sizeof(*state), MPI_BYTE, rank, 
            MPI_Aint_add(basedisp, circbuf_info.state_offset),
            sizeof(*state), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* get_elem: Insert element into remote buffer */
static void put_elem(MPI_Win win, MPI_Aint datadisp, int head, 
                     int rank, elem_t elem)
{
    MPI_Put(&elem, sizeof(elem), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem) * head),
            sizeof(elem), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* circbuf_insert: Insert element to the tail of buffer. */
static int circbuf_insert(MPI_Win win, elem_t elem, 
                          MPI_Aint basedisp, MPI_Aint datadisp, int rank)
{
    mutex_lock(win, basedisp, rank);

    /* State of remote buffer */
    circbuf_state_t state;

    get_circbuf_state(win, basedisp, rank, &state);

    printf("%d \t head = %d, tail = %d, size = %d\n", 
           myrank, state.head, state.tail, state.size);

    if (isfull(state)) {
        error_msg("can't insert element: buffer is full");
        return CODE_ERROR;
    }

    put_elem(win, datadisp, state.head, rank, elem);

    mutex_unlock(win, basedisp, rank);

    printf("result = %d\n", circbuf_info.lock_result);

    /*
     * 1. Get write address.
     *      -- get size
     *      -- get capacity
     * 2. Get element.
     * 3. CAS for address.
     *
     * Maybe Fetch_and_op??
     */

    return CODE_SUCCESS;
}

/* circbuf_remove: Remove element from buffer. */
/* static int circbuf_remove(circbuf_t *circbuf) */
/* { */
/*     return CODE_SUCCESS; */
/* } */

/* circbuf_free: Free memory and so on. */
static void circbuf_free(circbuf_t *circbuf)
{
    /* free(circbuf->buf); */
    /* free(circbuf); */

    MPI_Free_mem(circbuf->buf);
    MPI_Free_mem(circbuf);
}

/* circbuf_free: Initialize array for displaceemnts of all procs. */
static int disps_init(MPI_Aint **basedisps, MPI_Aint **datadisps,
                      circbuf_t *circbuf, int nproc)
{
    MPI_Alloc_mem(sizeof(MPI_Aint) * nproc, MPI_INFO_NULL, basedisps);
    MPI_Alloc_mem(sizeof(MPI_Aint) * nproc, MPI_INFO_NULL, datadisps);

    if ((*basedisps == NULL) || (*datadisps == NULL)) {
        error_msg("malloc() failed for circbuf->buf");
        return CODE_ERROR;
    }

    MPI_Allgather(&circbuf->basedisp, 1, MPI_AINT,
                  *basedisps, 1, MPI_AINT, MPI_COMM_WORLD);

    MPI_Allgather(&circbuf->datadisp, 1, MPI_AINT,
                  *datadisps, 1, MPI_AINT, MPI_COMM_WORLD);

    return CODE_SUCCESS;
}

/* disps_free: Free memory for displacements. */
static void disps_free(MPI_Aint *basedisps, MPI_Aint *datadisps)
{
    MPI_Free_mem(basedisps);
    MPI_Free_mem(datadisps);
}

/* circbuf_print: Print circbuf (useful for debug) */
static void circbuf_print(circbuf_t *circbuf, const char *label)
{
    printf("%d \t %s \t ", myrank, label);
    int i;
    for (i = 0; i < circbuf->state.size; i++) {
        printf("%d ", circbuf->buf[i]);
    }
    printf("\n");
}

int main(int argc, char *argv[]) 
{
    int nproc, rc;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);

    circbuf_t *circbuf;

    rc = circbuf_init(&circbuf, CIRCBUF_STARTSIZE);

    if (rc != CODE_SUCCESS) {
        error_msg("init_circbuf() failed");
        goto error_lbl;
    }

    MPI_Aint *basedisps, *datadisps;

    rc = disps_init(&basedisps, &datadisps, circbuf, nproc);

    if (rc != CODE_SUCCESS) {
        error_msg("disps_init() failed");
        goto error_lbl;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    printf("%d \t before \t lock = %d\n", myrank, circbuf->lock);

    /* if (myrank == 1) { */
        elem_t elem = myrank + 10;
        int remote_rank = (myrank + 1) % 2;
        circbuf_print(circbuf, "before");

        circbuf_insert(circbuf->win, elem, 
                       basedisps[remote_rank], datadisps[remote_rank], 
                       remote_rank);

        circbuf_print(circbuf, "after");
    /* } */

    MPI_Barrier(MPI_COMM_WORLD);

    printf("%d \t after all: \t lock = %d\n", myrank, circbuf->lock);

    /* int i; */
    /* elem_t elem = rank; */
    /* int remote_rank = 0; */
    /* for (i = 0; i < NELEM; i++) { */
    /*     circbuf_insert(circbuf, elem, disps[remote_rank], remote_rank); */
    /* } */
    
    circbuf_free(circbuf);
    disps_free(basedisps, datadisps);

error_lbl:
    MPI_Finalize();

    return 0;
}
