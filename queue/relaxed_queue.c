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
#include <stdbool.h>
#include <mpi.h>
#include <assert.h>

#include "relaxed_queue.h"
#include "utils.h"

circbuf_info_t circbuf_info;

extern bool ISDBG;

extern int myrank;

/* error_msg: Print error message. */
void error_msg(const char *msg, int _errno)
{
    fprintf(stderr, "%d \t %s", myrank, msg);
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
                  circbuf->basedisp, 1, MPI_AINT, circbuf->comm);

    MPI_Allgather(&circbuf->datadisp_local, 1, MPI_AINT,
                  circbuf->datadisp, 1, MPI_AINT, circbuf->comm);

    return CODE_SUCCESS;
}

static void init_random_generator(void)
{
    /* 10 because it's empirically better for two procs:
     * processes generate different sequences. */
    srandom(myrank * 10); 
}

static int get_rand(int maxval)
{
    return random() % maxval;
}

/* circbuf_init: Init circular buffer with specified size. */
int circbuf_init(circbuf_t **circbuf, int size, MPI_Comm comm)
{
    /* *circbuf = malloc(sizeof(circbuf_t)); */
    MPI_Alloc_mem(sizeof(circbuf_t), MPI_INFO_NULL, circbuf);
    if (*circbuf == NULL) {
        error_msg("malloc() failed for circbuf", errno);
        return CODE_ERROR;
    }

    (*circbuf)->comm = comm;

    MPI_Comm_size((*circbuf)->comm, &(*circbuf)->nproc);

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

    MPI_Win_create_dynamic(MPI_INFO_NULL, (*circbuf)->comm, &(*circbuf)->win);

    MPI_Win_attach((*circbuf)->win, *circbuf, sizeof(circbuf_t));
    MPI_Win_attach((*circbuf)->win, (*circbuf)->buf, size_bytes);

    int rc = disps_init(*circbuf, (*circbuf)->nproc);

    if (rc != CODE_SUCCESS) {
        error_msg("disps_init() failed", 0);
        return CODE_ERROR;
    }

    init_random_generator();

    (*circbuf)->ts_offset = mpi_sync_time(comm);

    (*circbuf)->nqueues_remove = NQUEUES_REMOVE;

    assert((*circbuf)->nproc >= (*circbuf)->nqueues_remove);

    return CODE_SUCCESS;
}

/* begin_RMA_epoch: Begin passive RMA access epoch. */
static void begin_RMA_epoch(MPI_Win win, int rank)
{
    /* MPI_Win_lock_all(0, win); */
    MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, win); 
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
    MPI_Get(state, sizeof(circbuf_state_t), MPI_BYTE, rank, 
            MPI_Aint_add(basedisp, circbuf_info.state_offset),
            sizeof(circbuf_state_t), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* put_elem: Insert element into remote buffer */
static void put_elem(MPI_Win win, MPI_Aint datadisp, int head, 
                     int rank, elem_t elem)
{
    MPI_Put(&elem, sizeof(elem_t), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem_t) * head),
            sizeof(elem_t), MPI_BYTE, win);

    MPI_Win_flush(rank, win);
}

/* get_elem: Get element from remote buffer */
static void get_elem(MPI_Win win, MPI_Aint datadisp, int tail, 
                     int rank, elem_t *elem)
{
    MPI_Get(elem, sizeof(elem_t), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem_t) * tail),
            sizeof(elem_t), MPI_BYTE, win);

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
     * 3. If circbuf is full, return with an error.
     * 4. Put element into buffer.
     * 5. Increment and refresh head pointer.
     * 6. Release lock. 
     */

    begin_RMA_epoch(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    circbuf_state_t state;

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    if (isfull(state)) {
        error_msg("circbuf_insert_proc() failed: buffer is full", 0);
        mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);
        end_RMA_epoch(circbuf->win, rank);
        return CODE_CIRCBUF_FULL;
    }

    put_elem(circbuf->win, circbuf->datadisp[rank], state.head, rank, elem);

    refresh_head(circbuf->win, circbuf->basedisp[rank], 
                 &state.head, state.size, rank);

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    end_RMA_epoch(circbuf->win, rank);

    return CODE_SUCCESS;
}

/* circbuf_remove: Remove an element from the circular buffer
 * on specified process. */
int circbuf_remove_proc(elem_t *elem, circbuf_t *circbuf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of circbuf.
     * 3. If circbuf is empty, return with an error.
     * 4. Get element from the buffer.
     * 5. Increment and refresh tail pointer.
     * 6. Release lock. 
     */

    begin_RMA_epoch(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    circbuf_state_t state;

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    if (isempty(state)) {
        error_msg("circbuf_remove_proc() failed: circbuf is empty", 0);
        mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);
        end_RMA_epoch(circbuf->win, rank);
        return CODE_CIRCBUF_EMPTY;
    }

    get_elem(circbuf->win, circbuf->datadisp[rank], state.tail, rank, elem);

    refresh_tail(circbuf->win, circbuf->basedisp[rank], 
                 &state.tail, state.size, rank);

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    end_RMA_epoch(circbuf->win, rank);

    return CODE_SUCCESS;
}

/* circbuf_fetch_elem: Get an element from proc's circbuf 
 * (not increment tail pointer and not finalize epoch and critical section). */
static int circbuf_fetch_elem(elem_t *elem, circbuf_state_t *state, 
                            circbuf_t *circbuf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of circbuf.
     * 3. If circbuf is empty, return with an error.
     * 4. Get element from the buffer.
     */

    begin_RMA_epoch(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, state);

    /* printf("%d \t rank %d: state.head = %d state.tail = %d\n",  */
    /*         myrank, rank, state->head, state->tail); */

    if (isempty(*state)) {
        error_msg("circbuf_get_elem() failed: circbuf is empty", 0);
        mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);
        end_RMA_epoch(circbuf->win, rank);
        return CODE_CIRCBUF_EMPTY;
    }

    get_elem(circbuf->win, circbuf->datadisp[rank], state->tail, rank, elem);

    return CODE_SUCCESS;
}

/* circbuf_get_elem_finalize: Complete all epochs, release lock
 * and optionally refresh tail. Remove_flag signs if element is removing. */
static void circbuf_get_elem_finalize(circbuf_state_t state, 
                                      circbuf_t *circbuf, int rank,
                                      bool remove_flag) 
{
    /*
     * 1. Increment and put tail pointer for the remote queue.
     * 2. Release lock.
     */

    if (remove_flag) {
        refresh_tail(circbuf->win, circbuf->basedisp[rank], 
                     &state.tail, state.size, rank);
    }

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->basedisp[rank], rank);

    end_RMA_epoch(circbuf->win, rank);
}

/* get_timestamp: Get current timestamp */
static double get_timestamp(void)
{
    return MPI_Wtime();
}

/* circbuf_insert: Choose randomly the queue and insert element into it. */
int circbuf_insert(val_t val, circbuf_t *circbuf)
{
    int rank = get_rand(circbuf->nproc);

    elem_t elem;
    elem.val = val;
    elem.ts = get_timestamp() + circbuf->ts_offset;

    /* printf("%d \t insert to %d\n", myrank, rank); */
    int rc = circbuf_insert_proc(elem, circbuf, rank);
    if (rc == CODE_CIRCBUF_FULL) {
        error_msg("circbuf_insert() failed: circbuf is full", 0);
        return CODE_CIRCBUF_FULL;
    }

    return CODE_SUCCESS;
}

/* isfound: Search for key in base. */
static bool isfound(int key, int *base, int nelem)
{
    for (int i = 0; i < nelem; i++)
        if (base[i] == key) {
            return true;
            printf("found %d\n", key);
        }
    return false;
}

/* circbuf_remove: */
int circbuf_remove(val_t *val, circbuf_t *circbuf)
{
    elem_t elem_cand[NQUEUES_REMOVE];
    circbuf_state_t states[NQUEUES_REMOVE];
    int ranks[NQUEUES_REMOVE];

    /* Number of queues for candidates */
    int nqueues_remove = circbuf->nqueues_remove;

    /* Total rest number of available queues */
    int avail_queues = circbuf->nproc;
    int i = 0;

    /* Random choose queues and get the candidate elements */
    while (i < nqueues_remove) {
        int rank = 0;
        /* FIXME Optimize this search&found place. */
        do {
            rank = get_rand(circbuf->nproc);
        } while (isfound(rank, ranks, i));
        
        int rc = circbuf_fetch_elem(&elem_cand[i], &states[i], circbuf, rank);

        if (rc == CODE_CIRCBUF_EMPTY) {
            avail_queues--;
            if (avail_queues < nqueues_remove) {
                nqueues_remove = avail_queues;
                
                if (nqueues_remove == 0) {
                    error_msg("circbuf_remove() failed: circbuf is empty", 0);
                    return CODE_CIRCBUF_EMPTY;
                }
            }
        } else {
            ranks[i] = rank;
            i++;
        }

        /* printf("%d \t remove from %d elem %d ts %f\n",  */
        /*         myrank, rank, elem_cand[i].val, elem_cand[i].ts); */
    }

    /* Compare candidate elements by timestamp and choose the with minimal
     * timestamp */
    double ts_min = elem_cand[0].ts;
    *val = elem_cand[0].val;
    int best_rank = ranks[0];
    for (int i = 1; i < nqueues_remove; i++) {
        if (elem_cand[i].ts < ts_min) {
            ts_min = elem_cand[i].ts;
            *val = elem_cand[i].val;
            best_rank = ranks[i];
        }
    }

    printf("%d \t rank %d is the best with ts %f\n", myrank, best_rank, ts_min);

    /* Finalize epochs and critical sections */
    for (int i = 0; i < nqueues_remove; i++) {
        bool remove_flag = (ranks[i] == best_rank);
        /* printf("rank %d remove_flag = %d bool %d\n", ranks[i], remove_flag, */
        /*         ranks[i] == best_rank); */
        circbuf_get_elem_finalize(states[i], circbuf, ranks[i], remove_flag);
    }

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
        printf("%d ", circbuf->buf[i].val);
    }
    printf("\n");
}
