/*
 * relaxed_queue.h Relaxed distributed queue implementation on MPI
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <mpi.h>

#include "relaxed_queue.h"

circbuf_info_t circbuf_info;

bool ISDBG = 0;

extern int myrank, nproc;

/* error_msg: Print error message. */
void error_msg(const char *msg, int _errno)
{
    fprintf(stderr, "%s", msg);
    if (_errno != 0)
        fprintf(stderr, ": %s", strerror(_errno));
    fprintf(stderr, "\n");
}

/* circbuf_info_init: Initialize process-oblivious information */
static void circbuf_info_init(void)
{
    circbuf_info.state_offset = offsetof(circbuf_t, state);
    circbuf_info.head_offset = circbuf_info.state_offset +
                               offsetof(circbuf_state_t, head);
    circbuf_info.tail_offset = circbuf_info.state_offset +
                               offsetof(circbuf_state_t, tail);
    circbuf_info.lock_state_offset = offsetof(circbuf_t, lock) + 
                                     offsetof(lock_t, state);
    circbuf_info.buf_offset = offsetof(circbuf_t, buf);
}

/* circbuf_free: Initialize array for displaceemnts of all procs. */
static int disps_init(circbuf_t *circbuf, int nproc)
{
    MPI_Alloc_mem(sizeof(MPI_Aint) * nproc, MPI_INFO_NULL, &circbuf->basedisp);
    MPI_Alloc_mem(sizeof(MPI_Aint) * nproc, MPI_INFO_NULL, &circbuf->datadisp);

    if ((circbuf->basedisp == NULL) || (circbuf->datadisp == NULL)) {
        error_msg("malloc() failed for circbuf->buf", errno);
        return CODE_ERROR;
    }

    MPI_Allgather(&circbuf->basedisp_local, 1, MPI_AINT,
                  circbuf->basedisp, 1, MPI_AINT, MPI_COMM_WORLD);

    MPI_Allgather(&circbuf->datadisp_local, 1, MPI_AINT,
                  circbuf->datadisp, 1, MPI_AINT, MPI_COMM_WORLD);

    return CODE_SUCCESS;
}

/* circbuf_init: Init circular buffer with specified size. */
int circbuf_init(circbuf_t **circbuf, int size)
{
    /* *circbuf = malloc(sizeof(circbuf_t)); */
    MPI_Alloc_mem(sizeof(circbuf_t), MPI_INFO_NULL, circbuf);
    if (*circbuf == NULL) {
        error_msg("malloc() failed for circbuf", errno);
        return CODE_ERROR;
    }

    MPI_Get_address(*circbuf, &(*circbuf)->basedisp_local);

    const int size_bytes = sizeof(elem_t) * size;

    /* (*circbuf)->buf = malloc(size_bytes); */
    MPI_Alloc_mem(size_bytes, MPI_INFO_NULL, &(*circbuf)->buf);
    if ((*circbuf)->buf == NULL) {
        error_msg("malloc() failed for circbuf->buf", errno);
        return CODE_ERROR;
    }

    memset((*circbuf)->buf, 0, size_bytes);

    MPI_Get_address((*circbuf)->buf, &(*circbuf)->datadisp_local);

    (*circbuf)->state.head = (*circbuf)->state.tail = 0;
    (*circbuf)->state.size = size;

    (*circbuf)->lock.state = (*circbuf)->lock.unlocked = 0;
    (*circbuf)->lock.locked = 1;
    (*circbuf)->lock.result = 0;

    circbuf_info_init();

    MPI_Win_create_dynamic(MPI_INFO_NULL, MPI_COMM_WORLD, &(*circbuf)->win);

    MPI_Win_attach((*circbuf)->win, *circbuf, sizeof(circbuf_t));
    MPI_Win_attach((*circbuf)->win, (*circbuf)->buf, size_bytes);

    int rc = disps_init(*circbuf, nproc);

    if (rc != CODE_SUCCESS) {
        error_msg("disps_init() failed", 0);
        return CODE_ERROR;
    }

    return CODE_SUCCESS;
}

/* begin_RMA_epoch: Begin passive RMA access epoch. */
static void begin_RMA_epoch(MPI_Win win, int rank)
{
    /* MPI_Win_lock_all(0, win); */
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, win); 
}

/* end_RMA_epoch: Complete passive RMA access epoch. */
static void end_RMA_epoch(MPI_Win win, int rank)
{
    /* MPI_Win_unlock_all(win); */
    MPI_Win_unlock(rank, win);
}

/* mutex_lock: */
static void mutex_lock(lock_t *lock, MPI_Win win, MPI_Aint basedisp, int rank)
{
    do {
        MPI_Compare_and_swap(&lock->locked, &lock->unlocked, 
                             &lock->result, MPI_INT, rank,
                             MPI_Aint_add(basedisp, 
                                          circbuf_info.lock_state_offset),
                             win);

        MPI_Win_flush(rank, win);
    } while (lock->result == 0); 
}

/* mutex_unlock: */
static void mutex_unlock(lock_t *lock, MPI_Win win, MPI_Aint basedisp, int rank)
{
    MPI_Compare_and_swap(&lock->unlocked, &lock->locked, 
                         &lock->result, MPI_INT, rank,
                         MPI_Aint_add(basedisp, 
                                      circbuf_info.lock_state_offset),
                         win);

    /* MPI_Win_flush(rank, win); */
}

/* isempty: Check if buffer is empty */
static bool isempty(circbuf_state_t state) 
{
    return state.head == state.tail;
}

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

/* put_elem: Insert element into remote buffer */
static void put_elem(MPI_Win win, MPI_Aint datadisp, int head, 
                     int rank, elem_t elem)
{
    MPI_Put(&elem, sizeof(elem), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem) * head),
            sizeof(elem), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* get_elem: Get element from remote buffer */
static void get_elem(MPI_Win win, MPI_Aint datadisp, int tail, 
                     int rank, elem_t *elem)
{
    MPI_Get(elem, sizeof(elem), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem) * tail),
            sizeof(elem), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* refresh_head: Increment head pointer and put to remote circbuf. */
static void refresh_head(MPI_Win win, MPI_Aint basedisp, int *head, int size, 
                         int rank)
{
    *head = (*head + 1) % size;

    MPI_Put(head, 1, MPI_INT, rank,
            MPI_Aint_add(basedisp, circbuf_info.head_offset),
            1, MPI_INT, win);

    MPI_Win_flush(rank, win);
}

/* refresh_head: Increment head pointer and put to remote circbuf. */
static void refresh_tail(MPI_Win win, MPI_Aint basedisp, int *tail, int size, 
                         int rank)
{
    *tail = (*tail + 1) % size;

    MPI_Put(tail, 1, MPI_INT, rank,
            MPI_Aint_add(basedisp, circbuf_info.tail_offset),
            1, MPI_INT, win);

    MPI_Win_flush(rank, win);
}

/* circbuf_insert: Insert an element to the tail of the circular buffer 
 * on specified process. */
int circbuf_insert_proc(elem_t elem, circbuf_t *circbuf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of circbuf.
     * 3. If circbuf is full, return.
     * 4. Put element into buffer.
     * 5. Increment and refresh buffer.
     * 6. Release lock. 
     */
    begin_RMA_epoch(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    if (!ISDBG) {
    circbuf_state_t state;  /* State of remote buffer */

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    printf("%d \t head = %d, tail = %d, size = %d\n", 
           myrank, state.head, state.tail, state.size);

    if (isfull(state)) {
        error_msg("Can't insert an element: buffer is full", 0);
        end_RMA_epoch(circbuf->win, rank);
        return CODE_ERROR;
    }

    put_elem(circbuf->win, circbuf->datadisp[rank], state.head, rank, elem);

    refresh_head(circbuf->win, circbuf->basedisp[rank], 
                 &state.head, state.size, rank);
    }

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    end_RMA_epoch(circbuf->win, rank);

    /* printf("result = %d\n", circbuf->lock.result); */

    return CODE_SUCCESS;
}

/* circbuf_remove: Remove an element from the circular buffer
 * on specified process. */
int circbuf_remove_proc(elem_t *elem, circbuf_t *circbuf, int rank)
{
    begin_RMA_epoch(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    circbuf_state_t state;  /* State of remote buffer */

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    /* printf("%d \t head = %d, tail = %d, size = %d\n",  */
    /*        myrank, state.head, state.tail, state.size); */

    if (isempty(state)) {
        error_msg("Can't remove an element: buffer is empty", 0);
        end_RMA_epoch(circbuf->win, rank);
        return CODE_ERROR;
    }

    get_elem(circbuf->win, circbuf->datadisp[rank], state.tail, rank, elem);

    refresh_tail(circbuf->win, circbuf->basedisp[rank], 
                 &state.tail, state.size, rank);

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    end_RMA_epoch(circbuf->win, rank);

    /* printf("result = %d\n", circbuf->lock.result); */

    return CODE_SUCCESS;
}

/* circbuf_free: Free memory and so on. */
void circbuf_free(circbuf_t *circbuf)
{
    /* free(circbuf->buf); */
    /* free(circbuf); */

    MPI_Free_mem(circbuf->buf);
    MPI_Free_mem(circbuf);
    MPI_Free_mem(circbuf->basedisp);
    MPI_Free_mem(circbuf->datadisp);
}

/* circbuf_print: Print circbuf (useful for debug) */
void circbuf_print(circbuf_t *circbuf, const char *label)
{
    printf("%d \t %s \t ", myrank, label);
    int i;
    for (i = 0; i < circbuf->state.size; i++) {
        if (circbuf->state.head == i)
            printf("(h)");
        if (circbuf->state.tail == i)
            printf("(t)");
        printf("%d ", circbuf->buf[i]);
    }
    printf("\n");
}
