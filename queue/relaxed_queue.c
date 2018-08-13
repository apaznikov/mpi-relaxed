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

/* disps_init: Initialize array for displaceemnts of all procs. */
static int disps_init(circbuf_t *circbuf)
{
    MPI_Alloc_mem(sizeof(MPI_Aint) * circbuf->nproc, 
                  MPI_INFO_NULL, &circbuf->basedisp);
    MPI_Alloc_mem(sizeof(MPI_Aint) * circbuf->nproc, 
                  MPI_INFO_NULL, &circbuf->datadisp);
    MPI_Alloc_mem(sizeof(MPI_Aint) * circbuf->nproc, 
                  MPI_INFO_NULL, &circbuf->lockdisp);

    if ((circbuf->basedisp == NULL) || (circbuf->datadisp == NULL)) {
        error_msg("malloc() failed for circbuf->buf", errno);
        return CODE_ERROR;
    }

    MPI_Allgather(&circbuf->basedisp_local, 1, MPI_AINT,
                  circbuf->basedisp, 1, MPI_AINT, circbuf->comm);

    MPI_Allgather(&circbuf->datadisp_local, 1, MPI_AINT,
                  circbuf->datadisp, 1, MPI_AINT, circbuf->comm);

    for (int rank = 0; rank < circbuf->nproc; rank++) {
        circbuf->lockdisp[rank] = MPI_Aint_add(circbuf->basedisp[rank],
                                               circbuf_info.lock_state_offset);
    }

    return CODE_SUCCESS;
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

    (*circbuf)->lock.state = (*circbuf)->lock.unlocked = LOCK_UNLOCKED;
    (*circbuf)->lock.locked = LOCK_LOCKED;
    (*circbuf)->lock.result = LOCK_UNLOCKED;

    circbuf_info_init();

    MPI_Win_create_dynamic(MPI_INFO_NULL, (*circbuf)->comm, &(*circbuf)->win);

    MPI_Win_attach((*circbuf)->win, *circbuf, sizeof(circbuf_t));
    MPI_Win_attach((*circbuf)->win, (*circbuf)->buf, size_bytes);

    int rc = disps_init(*circbuf);

    if (rc != CODE_SUCCESS) {
        error_msg("disps_init() failed", 0);
        return CODE_ERROR;
    }

    init_random_generator();

    (*circbuf)->ts_offset = mpi_sync_time(comm);

    if ((*circbuf)->nproc > NQUEUES_REMOVE) {
        (*circbuf)->nqueues_remove = NQUEUES_REMOVE;
    } else if ((*circbuf)->nproc > 1) {
        /* Case when nproc == nqueues_remove cause deadlock! */
        (*circbuf)->nqueues_remove = (*circbuf)->nproc - 1;
    } else {
        (*circbuf)->nqueues_remove = 1;
    }

    /* printf("nqueues = %d\n", (*circbuf)->nqueues_remove); */

    (*circbuf)->max_attempts = (*circbuf)->nproc;

    return CODE_SUCCESS;
}

/* begin_RMA_epoch_one: Begin passive RMA access epoch with specified proc. */
static void begin_RMA_epoch_one(MPI_Win win, int rank)
{
    /* MPI_Win_lock_all(0, win); */
    MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, win); 
    /* EXCLUSIVE better */
    /* MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, win);  */
}

/* begin_RMA_epoch_all: Begin passive RMA access epoch with all proc. */
static void begin_RMA_epoch_all(MPI_Win win)
{
    MPI_Win_lock_all(0, win);
}

/* end_RMA_epoch_one: Complete passive RMA access epoch with specified proc. */
static void end_RMA_epoch_one(MPI_Win win, int rank)
{
    /* MPI_Win_unlock_all(win); */
    MPI_Win_unlock(rank, win);
}

/* end_RMA_epoch_all: Complete passive RMA access epoch with all proc. */
static void end_RMA_epoch_all(MPI_Win win)
{
    MPI_Win_unlock_all(win);
}

/* mutex_lock: */
static void mutex_lock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    do {
        /* static int ii = 0; */
        /* ii++; */
        /* if (ii % 1000000 == 0) */
        /*     printf("%d \t try to change rank %d from %d to %d\n",  */
        /*             myrank, rank, lock->unlocked, lock->locked); */

        MPI_Compare_and_swap(&lock->locked, &lock->unlocked, 
                             &lock->result, MPI_INT, rank,
                             lockdisp,
                             /* MPI_Aint_add(basedisp,  */
                             /*              circbuf_info.lock_state_offset), */
                             win);

        MPI_Win_flush(rank, win);
    } while (lock->result == lock->locked); 
    /* printf("%d \t success!\n", myrank); */

    if (ISDBG) {
        printf("%d \t CAS lock %d\n", myrank, lock->result);

        printf("%d \t acquire %d\n", myrank, rank);
    }
}

/* mutex_trylock: */ 
static int mutex_trylock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank) 
{
    MPI_Compare_and_swap(&lock->locked, &lock->unlocked, 
                         &lock->result, MPI_INT, rank,
                         lockdisp,
                         /* MPI_Aint_add(basedisp, circbuf_info.lock_state_offset), */
                         win);

    MPI_Win_flush(rank, win);

    if (lock->result == lock->unlocked) {
        if (ISDBG) {
            printf("%d \t CAS lock %d\n", myrank, lock->result);

            printf("%d \t acquire %d\n", myrank, rank);
        }
        return CODE_TRYLOCK_SUCCESS;
    } else {
        return CODE_TRYLOCK_BUSY;
    }
}

/* mutex_unlock: */
static void mutex_unlock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    /* MPI_Compare_and_swap(&lock->unlocked, &lock->locked,  */
    /*                      &lock->result, MPI_INT, rank, */
    /*                      MPI_Aint_add(basedisp,  */
    /*                                   circbuf_info.lock_state_offset), */
    /*                      win); */

    /* MPI_Win_flush(rank, win);       #<{(| DEBUG |)}># */

    MPI_Accumulate(&lock->unlocked, 
                   1, MPI_INT, rank,
                   /* MPI_Aint_add(basedisp, circbuf_info.lock_state_offset), */
                   lockdisp,
                   1, MPI_INT, MPI_REPLACE, win);

    /* MPI_Win_flush(rank, win);       #<{(| DEBUG |)}># */

    if (ISDBG) {
        printf("%d \t release %d\n", myrank, rank);
    }
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

    /* Flush, because we will use circbuf state in this epoch */
    MPI_Win_flush(rank, win);
}

/* put_elem: Insert element into remote buffer */
static void put_elem(MPI_Win win, MPI_Aint datadisp, int head, 
                     int rank, elem_t elem)
{
    MPI_Put(&elem, sizeof(elem_t), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem_t) * head),
            sizeof(elem_t), MPI_BYTE, win);

    /* MPI_Win_flush(rank, win); */
}

/* get_elem: Get element from remote buffer */
static void get_elem(MPI_Win win, MPI_Aint datadisp, int tail, 
                     int rank, elem_t *elem)
{
    MPI_Get(elem, sizeof(elem_t), MPI_BYTE, rank,
            MPI_Aint_add(datadisp, sizeof(elem_t) * tail),
            sizeof(elem_t), MPI_BYTE, win);

    /* Flush, because we will use elem in this epoch */
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

    /* MPI_Win_flush(rank, win); */
}

/* refresh_tail: Increment tail pointer and put to remote circbuf. */
static void refresh_tail(MPI_Win win, MPI_Aint basedisp, int *tail, int size, 
                         int rank)
{
    *tail = (*tail + 1) % size;

    MPI_Put(tail, 1, MPI_INT, rank,
            MPI_Aint_add(basedisp, circbuf_info.tail_offset),
            1, MPI_INT, win);

    /* MPI_Win_flush(rank, win); */
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

    begin_RMA_epoch_one(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);

    circbuf_state_t state;

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    if (isfull(state)) {
        error_msg("circbuf_insert_proc() failed: buffer is full", 0);
        mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);
        end_RMA_epoch_one(circbuf->win, rank);
        return CODE_CIRCBUF_FULL;
    }

    put_elem(circbuf->win, circbuf->datadisp[rank], state.head, rank, elem);

    refresh_head(circbuf->win, circbuf->basedisp[rank], 
                 &state.head, state.size, rank);

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);

    end_RMA_epoch_one(circbuf->win, rank);

    return CODE_SUCCESS;
}

/* circbuf_tryinsert: Try to acquire lock and insert an element to the tail. */
int circbuf_tryinsert_proc(elem_t elem, circbuf_t *circbuf, int rank)
{
    /*
     * 1. Try to lock mutex. If success, continue.
     * 2. Remotely get state of circbuf.
     * 3. If circbuf is full, return with an error.
     * 4. Put element into buffer.
     * 5. Increment and refresh head pointer.
     * 6. Release lock. 
     */

    begin_RMA_epoch_one(circbuf->win, rank);

    int rc = mutex_trylock(&circbuf->lock, circbuf->win, 
                           circbuf->lockdisp[rank], rank);

    if (rc == CODE_TRYLOCK_SUCCESS) {
        circbuf_state_t state;

        get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

        if (isfull(state)) {
            error_msg("circbuf_insert_proc() failed: buffer is full", 0);
            mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);
            end_RMA_epoch_one(circbuf->win, rank);
            return CODE_CIRCBUF_FULL;
        }

        put_elem(circbuf->win, circbuf->datadisp[rank], state.head, rank, elem);

        refresh_head(circbuf->win, circbuf->basedisp[rank], 
                     &state.head, state.size, rank);

        mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);
    }

    end_RMA_epoch_one(circbuf->win, rank);

    if (rc == CODE_TRYLOCK_SUCCESS)
        return CODE_SUCCESS;
    else
        return CODE_CIRCBUF_BUSY;
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

    begin_RMA_epoch_one(circbuf->win, rank);

    mutex_lock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);

    circbuf_state_t state;

    get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, &state);

    if (isempty(state)) {
        error_msg("circbuf_remove_proc() failed: circbuf is empty", 0);
        mutex_unlock(&circbuf->lock, circbuf->win,circbuf->lockdisp[rank],rank);
        end_RMA_epoch_one(circbuf->win, rank);
        return CODE_CIRCBUF_EMPTY;
    }

    get_elem(circbuf->win, circbuf->datadisp[rank], state.tail, rank, elem);

    refresh_tail(circbuf->win, circbuf->basedisp[rank], 
                 &state.tail, state.size, rank);

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);

    end_RMA_epoch_one(circbuf->win, rank);

    return CODE_SUCCESS;
}

/* circbuf_tryfetch_elem: Try to lock mutex and get an element from proc's 
 * circbuf (not increment tail pointer and not finalize epoch 
 * and critical section). */
static int circbuf_tryfetch_elem(elem_t *elem, circbuf_state_t *state, 
                            circbuf_t *circbuf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of circbuf.
     * 3. If circbuf is empty, return with an error.
     * 4. Get element from the buffer.
     */

    int rc = mutex_trylock(&circbuf->lock, circbuf->win, 
                           circbuf->lockdisp[rank], rank);

    if (rc == CODE_TRYLOCK_SUCCESS) {
        get_circbuf_state(circbuf->win, circbuf->basedisp[rank], rank, state);

        /* printf("%d \t rank %d: state.head = %d state.tail = %d\n",  */
        /*         myrank, rank, state->head, state->tail); */

        if (isempty(*state)) {
            error_msg("circbuf_get_elem() failed: circbuf is empty", 0);
            mutex_unlock(&circbuf->lock, circbuf->win, 
                         circbuf->lockdisp[rank], rank);
            return CODE_CIRCBUF_EMPTY;
        }

        get_elem(circbuf->win, circbuf->datadisp[rank], state->tail, 
                 rank, elem);

        return CODE_SUCCESS;
    } else {
        return CODE_CIRCBUF_BUSY;
    }

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

    mutex_unlock(&circbuf->lock, circbuf->win, circbuf->lockdisp[rank], rank);
}

/* get_timestamp: Get current timestamp */
static double get_timestamp(void)
{
    return MPI_Wtime();
}

/* circbuf_insert: Choose randomly the queue and insert element into it. */
int circbuf_insert(val_t val, circbuf_t *circbuf)
{
    int rc;

    /* Total rest number of available queues */
    int avail_queues = circbuf->nproc;

    do {
        int rank = get_rand(circbuf->nproc);

        elem_t elem;
        elem.val = val;
        elem.ts = get_timestamp() + circbuf->ts_offset;

        rc = circbuf_insert_proc(elem, circbuf, rank);

        if (rc == CODE_CIRCBUF_FULL) {
            avail_queues--;

            if (avail_queues == 0) {
                error_msg("circbuf_insert() failed: circbuf is full", 0);
                return CODE_CIRCBUF_FULL;
            }
        }
    } while ((rc == CODE_CIRCBUF_BUSY) || (rc == CODE_CIRCBUF_FULL));

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
    int curr_nqueues = 0;

    /* Number of attempts to lock the mutex */
    int nattempts = 0;

    begin_RMA_epoch_all(circbuf->win);

    /* Random choose queues and get the candidate elements */
    while (curr_nqueues < nqueues_remove) {
        int rank = 0;
        /* FIXME Optimize this search&found place. */
        do {
            rank = get_rand(circbuf->nproc);
        } while (isfound(rank, ranks, curr_nqueues));

        /* begin_RMA_epoch_one(circbuf->win, rank); */
        
        int rc = circbuf_tryfetch_elem(&elem_cand[curr_nqueues], 
                                       &states[curr_nqueues], circbuf, rank);

        if (rc == CODE_SUCCESS) { 
            /* end_RMA_epoch_one(circbuf->win, rank); */
            ranks[curr_nqueues] = rank;
            curr_nqueues++;
            nattempts = 0;
        } else if (rc == CODE_CIRCBUF_BUSY) {
            /* If lock is busy, try another process */
            /* printf("%d \t BUSY %d\n", myrank, rank); */

            /* Count number of attempts to lock the mutex
             * to avoid deadlock (only for 2nd and following
             * queues */
            if (curr_nqueues > 0) {
                nattempts++;

                /* If number of attempts is too large,
                 * we suppose it's a deadlock and unlock
                 * all locked queues 
                 * FIXME not all locked queues but some of them? */
                if (nattempts > circbuf->max_attempts) {
                    /* printf("%d \t DEADLOCK?\n", myrank); */
                    for (int i = 0; i < curr_nqueues; i++) {
                        mutex_unlock(&circbuf->lock, circbuf->win, 
                                     circbuf->lockdisp[ranks[i]], ranks[i]);
                    }
                    curr_nqueues = 0;
                }
            }

            continue;
        } else if (rc == CODE_CIRCBUF_EMPTY) {
            /* printf("%d \t EMPTY rank %d\n", myrank, rank); */

            avail_queues--;
            if (avail_queues < nqueues_remove) {
                nqueues_remove = avail_queues;
                
                if (nqueues_remove == 0) {
                    /* end_RMA_epoch_one(circbuf->win, rank); */
                    end_RMA_epoch_all(circbuf->win);
                    error_msg("circbuf_remove() failed: circbuf is empty", 0);
                    return CODE_CIRCBUF_EMPTY;
                }
            }
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

    /* printf("%d \t rank %d is the best with ts %f\n", myrank, best_rank, ts_min); */

    /* Finalize epochs and critical sections */
    for (int i = 0; i < nqueues_remove; i++) {
        bool remove_flag = (ranks[i] == best_rank);
        /* printf("rank %d remove_flag = %d bool %d\n", ranks[i], remove_flag, */
        /*         ranks[i] == best_rank); */
        circbuf_get_elem_finalize(states[i], circbuf, ranks[i], remove_flag);

        /* end_RMA_epoch_one(circbuf->win, ranks[i]); */
    }

    end_RMA_epoch_all(circbuf->win);

    return CODE_SUCCESS;
}


/* circbuf_free: Free memory and so on. */
void circbuf_free(circbuf_t *circbuf)
{
    /* free(circbuf->buf); */
    /* free(circbuf); */
    MPI_Barrier(circbuf->comm);

    MPI_Free_mem(circbuf->buf);
    MPI_Free_mem(circbuf);
    MPI_Free_mem(circbuf->basedisp);
    MPI_Free_mem(circbuf->datadisp);
    MPI_Free_mem(circbuf->lockdisp);
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
