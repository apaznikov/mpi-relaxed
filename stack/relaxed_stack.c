/*
 * relaxed_stack.c: Relaxed distributed stack implementation on MPI
 * 
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com>
 * (C) 2019 Aleksandr Polozhenskii <polozhenskii@gmail.com>
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <stdbool.h>
#include <unistd.h>
#include <mpi.h>

#include "relaxed_stack.h"
#include "utils.h"

buf_info_t buf_info;

extern bool ISDBG;

extern int myrank;

// error_msg: Print error message.
void error_msg(const char *msg, int _errno)
{
    fprintf(stderr, "%d \t %s", myrank, msg);
    if (_errno != 0) {
        fprintf(stderr, ": %s", strerror(_errno));
    }
    fprintf(stderr, "\n");
}

// buf_info_init: Init process-oblivious information.
static void buf_info_init(void)
{
    buf_info.state_offset = offsetof(buf_t, state);
    buf_info.top_offset = buf_info.state_offset + offsetof(buf_state_t, top);
    buf_info.lock_state_offset = offsetof(buf_t, lock) + offsetof(lock_t, state);
    buf_info.buf_offset = offsetof(buf_t, data);
}

// disps_init: Initialize array for displacements of all processes.
static int disps_init(buf_t *buf)
{
    MPI_Alloc_mem(sizeof(MPI_Aint) * buf->nproc, MPI_INFO_NULL, &buf->basedisp);
    MPI_Alloc_mem(sizeof(MPI_Aint) * buf->nproc, MPI_INFO_NULL, &buf->datadisp);
    MPI_Alloc_mem(sizeof(MPI_Aint) * buf->nproc, MPI_INFO_NULL, &buf->lockdisp);

    if ((buf->basedisp == NULL) || (buf->datadisp == NULL) || (buf->lockdisp == NULL)) {
        error_msg("MPI_Alloc_mem() failed for buf", errno);
        return CODE_ERROR;
    }

    MPI_Allgather(&buf->basedisp_local, 1, MPI_AINT, buf->basedisp, 1, MPI_AINT, buf->comm);
    MPI_Allgather(&buf->datadisp_local, 1, MPI_AINT, buf->datadisp, 1, MPI_AINT, buf->comm);

    int rank;
    for (rank = 0; rank < buf->nproc; rank++) {
        buf->lockdisp[rank] = MPI_Aint_add(buf->basedisp[rank], buf_info.lock_state_offset);
    }

    return CODE_SUCCESS;
}

// buf_init: Init buffer with specified size.
int buf_init(buf_t **buf, int size, MPI_Comm comm)
{
    MPI_Alloc_mem(sizeof(buf_t), MPI_INFO_NULL, buf);
    if (*buf == NULL) {
        error_msg("MPI_Alloc_mem() failed for buf", errno);
        return CODE_ERROR;
    }

    (*buf)->comm = comm;

    MPI_Comm_size((*buf)->comm, &(*buf)->nproc);

    MPI_Get_address(*buf, &(*buf)->basedisp_local);

    const int size_bytes = sizeof(elem_t) * size;
    //printf("size = %d\n", size_bytes);

    // Physical buffer memory allocation
    MPI_Alloc_mem(size_bytes, MPI_INFO_NULL, &(*buf)->data);
    if ((*buf)->data == NULL) {
        error_msg("MPI_Alloc_mem() failed for buf->data", errno);
        return CODE_ERROR;
    }

    memset((*buf)->data, 0, size_bytes);

    MPI_Get_address((*buf)->data, &(*buf)->datadisp_local);

    // Init buffer state
    (*buf)->state.top = 0;
    (*buf)->state.size = size;

    // Spinlock init
    (*buf)->lock.state = (*buf)->lock.unlocked = LOCK_UNLOCKED;
    (*buf)->lock.locked = LOCK_LOCKED;
    (*buf)->lock.result = LOCK_UNLOCKED;

    buf_info_init();

    MPI_Win_create_dynamic(MPI_INFO_NULL, (*buf)->comm, &(*buf)->win);

    MPI_Win_attach((*buf)->win, *buf, sizeof(buf_t));
    MPI_Win_attach((*buf)->win, (*buf)->data, size_bytes);

    int rc = disps_init(*buf);
    if (rc != CODE_SUCCESS) {
        error_msg("disps_init() failed", 0);
    }

    init_random_generator();

    (*buf)->ts_offset = mpi_sync_time(comm);

    if ((*buf)->nproc > NSTACKS_REMOVE) {
        (*buf)->nstacks_remove = NSTACKS_REMOVE;
    } else if ((*buf)->nproc > 1) {
        (*buf)->nstacks_remove = (*buf)->nproc - 1;
    } else {
        (*buf)->nstacks_remove = 1;
    }

    (*buf)->max_attempts = (*buf)->nproc;

    return CODE_SUCCESS;
}

// begin_RMA_epoch_one: Begin passive RMA access epoch with specified process.
static void begin_RMA_epoch_one(MPI_Win win, int rank)
{
    //MPI_Win_lock(MPI_LOCK_SHARED, rank, 0, win);
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, win);
}

// begin_RMA_epoch_all: Begin passive RMA access epoch with all processes.
static void begin_RMA_epoch_all(MPI_Win win)
{
    MPI_Win_lock_all(0, win);
}

// end_RMA_epoch_one: Complete passive RMA access epoch with specified process.
static void end_RMA_epoch_one(MPI_Win win, int rank)
{
    MPI_Win_unlock(rank, win);
}

// end_RMA_epoch_all: Complete passive RMA access epoch with all processes.
static void end_RMA_epoch_all(MPI_Win win)
{
    MPI_Win_unlock_all(win);
}

// mutex_lock: Test and set lock.
static void mutex_lock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    do {
        MPI_Compare_and_swap(&lock->locked, &lock->unlocked,
                             &lock->result, MPI_INT, rank, lockdisp, win);
        MPI_Win_flush(rank, win);
    } while (lock->result == lock->locked);

    if (ISDBG) {
        printf("%d \t CAS lock %d\n", myrank, lock->result);
        printf("%d \t acquire %d\n", myrank, rank);
    }
}

// mutex_trylock: Try to lock mutex and return code.
static int mutex_trylock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    MPI_Compare_and_swap(&lock->locked, &lock->unlocked,
                         &lock->result, MPI_INT, rank, lockdisp, win);

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

// mutex_unlock: Unlock mutex.
static void mutex_unlock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    MPI_Accumulate(&lock->unlocked, 1, MPI_INT, rank, lockdisp, 1, MPI_INT, MPI_REPLACE, win);

    if (ISDBG) {
        printf("%d \t release %d\n", myrank, rank);
    }
}

// get_lock_state: Get lock state of the specified process.
static int get_lock_state(MPI_Win win, MPI_Aint lockdisp, int rank)
{
    int result;
    int *tmp = 0;

    MPI_Fetch_and_op(tmp, &result, MPI_INT, rank, lockdisp, MPI_NO_OP, win);
    MPI_Win_flush(rank, win);

    return result;
}

// mutex_ttas_lock: Test and test-and-test lock.
static void mutex_ttas_lock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    // *** DEBUG ***
    // int lock_state = get_lock_state(win, lockdisp, rank);
    // printf("P%d: lock_state = %d\n", rank, lock_state);

    do {
        while (get_lock_state(win, lockdisp, rank)) {
            // printf("P%d: %d rank is locked. Lurking...\n", myrank, rank);
            continue;
        }

        MPI_Compare_and_swap(&lock->locked, &lock->unlocked,
                             &lock->result, MPI_INT, rank, lockdisp, win);
        MPI_Win_flush(rank, win);
    } while (lock->result == lock->locked); 

    if (ISDBG) {
        printf("%d \t CAS lock %d\n", myrank, lock->result);
        printf("%d \t acquire %d\n", myrank, rank);
    }
}

// mutex_backoff_lock: Exponential Backoff Lock.
static int mutex_backoff_lock(lock_t *lock, MPI_Win win, MPI_Aint lockdisp, int rank)
{
    // *** DEBUG ***
    // int lock_state = get_lock_state(win, lockdisp, rank);
    // printf("P%d: lock_state = %d\n", rank, lock_state);

    int delay = MIN_DELAY;

    for (;;) {
        while (get_lock_state(win, lockdisp, rank)) {
            // printf("P%d: %d rank is locked. Lurking...\n", myrank, rank);
            continue;
        }

        MPI_Compare_and_swap(&lock->locked, &lock->unlocked,
                             &lock->result, MPI_INT, rank, lockdisp, win);
        MPI_Win_flush(rank, win);

        if (lock->result == lock->unlocked) {
            if (ISDBG) {
                printf("%d \t CAS lock %d\n", myrank, lock->result);
                printf("%d \t acquire %d\n", myrank, rank);
            }
            return CODE_SUCCESS;
        }

        usleep(get_rand(delay));

        if (delay < MAX_DELAY) {
            delay *= 2;
        }
    }
}

// isempty: Check if buffer is empty.
static bool isempty(buf_state_t state)
{
    return state.top == 0;
}

// isfull: Check if buffer is full.
static bool isfull(buf_t *buf, buf_state_t state)
{
    return state.top == state.size;
}

// get_buf_state: Get state of remote buffer.
static void get_buf_state(MPI_Win win, MPI_Aint basedisp, int rank, buf_state_t *state)
{
    elem_t tmp_result;
    MPI_Get(state, sizeof(buf_state_t), MPI_BYTE, rank,
            MPI_Aint_add(basedisp, buf_info.state_offset),
            sizeof(buf_state_t), MPI_BYTE, win);
    
    // Flush, because we will use elem in this epoch
    MPI_Win_flush(rank, win);
}

// put_elem: Insert element into remote buffer.
static void put_elem(buf_t *buf, MPI_Win win, MPI_Aint datadisp, int top,
                     int rank, elem_t elem)
{
    MPI_Accumulate(&elem, sizeof(elem_t), MPI_BYTE, rank,
                   MPI_Aint_add(datadisp, sizeof(elem_t) * top),
                   sizeof(elem_t), MPI_BYTE, MPI_REPLACE, win);
}

// get_elem: Get element from remote buffer
static void get_elem(MPI_Win win, MPI_Aint datadisp, int top,
                     int rank, elem_t *elem)
{   
    elem_t tmp_result;
    MPI_Get_accumulate(elem, sizeof(elem_t), MPI_BYTE,
                       &tmp_result, sizeof(elem_t), MPI_BYTE, rank,
                       MPI_Aint_add(datadisp, sizeof(elem_t) * top),
                       sizeof(elem_t), MPI_BYTE, MPI_NO_OP, win);
    

    // Flush, because we will use elem in this epoch
    MPI_Win_flush(rank, win);
}

// dec_top: Decrement top pointer and put to remote buffer.
static void dec_top(MPI_Win win, MPI_Aint basedisp, int *top, int size,
                        int rank)
{
    *top = *top - 1;

    MPI_Accumulate(top, 1, MPI_INT, rank,
                   MPI_Aint_add(basedisp, buf_info.top_offset),
                   1, MPI_INT, MPI_REPLACE, win);
}

// inc_top: Increment top pointer and put to remote buffer.
static void inc_top(MPI_Win win, MPI_Aint basedisp, int *top, int size,
                        int rank)
{
    *top = *top + 1;
    
    MPI_Accumulate(top, 1, MPI_INT, rank,
                   MPI_Aint_add(basedisp, buf_info.top_offset),
                   1, MPI_INT, MPI_REPLACE, win);
}

// buf_push_proc: Push an element to the top of the buffer
// on specified process.
int buf_push_proc(elem_t elem, buf_t *buf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of buffer.
     * 3. If buffer is full, return with an error.
     * 4. Put element into buffer.
     * 5. Increment and refresh top pointer.
     * 6. Release lock.
     */

    begin_RMA_epoch_one(buf->win, rank);
    
    // mutex_trylock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
    mutex_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    // TTAS & Backoff locks
    // mutex_ttas_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
    // mutex_backoff_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    buf_state_t state;

    get_buf_state(buf->win, buf->basedisp[rank], rank, &state);

    if (isfull(buf, state)) {
        error_msg("buf_push_proc() failed: buffer is full", 0);

        mutex_unlock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
        end_RMA_epoch_one(buf->win, rank);

        return CODE_BUFFER_FULL;
    }

    put_elem(buf, buf->win, buf->datadisp[rank], state.top, rank, elem);

    inc_top(buf->win, buf->basedisp[rank],
                &state.top, state.size, rank);
    
    mutex_unlock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    end_RMA_epoch_one(buf->win, rank);

    return CODE_SUCCESS;
}

// buf_pop_proc: Remove an element from the buffer
// on specified process.
int buf_pop_proc(elem_t *elem, buf_t *buf, int rank)
{
    /*
     * 1. Acquire lock.
     * 2. Remotely get state of buffer.
     * 3. If buffer is empty, return with an error.
     * 4. Get an element from the buffer.
     * 5. Decrement and refresh top pointer.
     * 6. Release lock.
     */

    begin_RMA_epoch_one(buf->win, rank);

    // mutex_trylock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
    mutex_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    // TTAS & Backoff locks
    // mutex_ttas_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
    // mutex_backoff_lock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    buf_state_t state;

    get_buf_state(buf->win, buf->basedisp[rank], rank, &state);

    if (isempty(state)) {
        error_msg("buf_pop_proc() failed: buffer is empty", 0);
        mutex_unlock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
        end_RMA_epoch_one(buf->win, rank);
        return CODE_BUFFER_EMPTY;
    }

    get_elem(buf->win, buf->datadisp[rank], state.top, rank, elem);

    dec_top(buf->win, buf->basedisp[rank],
                &state.top, state.size, rank);

    mutex_unlock(&buf->lock, buf->win, buf->lockdisp[rank], rank);

    end_RMA_epoch_one(buf->win, rank);

    return CODE_SUCCESS;
}

// buf_tryfetch_elem: Try to lock mutex and get an element from proc's
// buffer (not increment top pointer and finalize epoch and critical section).
static int buf_tryfetch_elem(elem_t *elem, buf_state_t *state,
                             buf_t *buf, int rank)
{
    /* 1. Acquire lock.
     * 2. Remotely get state of buffer.
     * 3. If buffer is empty, return with an error.
     * 4. Get element from the buffer.
     */

    int rc = mutex_trylock(&buf->lock, buf->win, 
                            buf->lockdisp[rank], rank);

    if (rc == CODE_TRYLOCK_SUCCESS) {
        get_buf_state(buf->win, buf->basedisp[rank], rank, state);

        if (isempty(*state)) {
            error_msg("buf_tryfetch_elem() failed: buffer is empty", 0);
            mutex_unlock(&buf->lock, buf->win, 
                         buf->lockdisp[rank], rank);
            return CODE_BUFFER_EMPTY;                         
        }

        get_elem(buf->win, buf->datadisp[rank], state->top,
                 rank, elem);
        
        return CODE_SUCCESS;
    } else {
        return CODE_BUFFER_BUSY;
    } 
}

// buf_get_elem_finalize: Complete all epochs, release lock
// and optionally refresh top. Remove_flag signs if element is removing.
static void buf_get_elem_finalize(buf_state_t state,
                                  buf_t *buf, int rank,
                                  bool remove_flag)
{
    /*
     * 1. Decrement and put top pointer for the remote stack.
     * 2. Release lock.
     */

    if (remove_flag) {
        dec_top(buf->win, buf->basedisp[rank],
                    &state.top, state.size, rank);
    }

    mutex_unlock(&buf->lock, buf->win, buf->lockdisp[rank], rank);
}                                  

// get_timestamp: Get current timestamp
static double get_timestamp(void)
{
    return MPI_Wtime();
}

// buf_push: Choose randomly the stack and push element into it.
int buf_push(val_t val, buf_t *buf)
{
    int rc;
    int avail_stacks = buf->nproc;

    do {
        int rank = get_rand(buf->nproc);

        elem_t elem;
        elem.val = val;
        elem.ts = get_timestamp() + buf->ts_offset;

        rc = buf_push_proc(elem, buf, rank);

        if (rc == CODE_BUFFER_FULL) {
            avail_stacks--;

            if (avail_stacks == 0) {
                error_msg("buf_push() failed: buf is full", 0);
                return CODE_BUFFER_FULL;
            }
        }
    } while ((rc == CODE_BUFFER_BUSY) || (rc == CODE_BUFFER_FULL));
    
    return CODE_SUCCESS;
}

// isfound: Search for key in base.
static bool isfound(int key, int *base, int nelem)
{
    int i;
    for (i = 0; i < nelem; i++) {
        if (base[i] == key) {
            return true;
        }
    }
    return false;
}

// buf_pop: Pop an element from stack. 
int buf_pop(val_t *val, buf_t *buf)
{
    elem_t elem_cand[NSTACKS_REMOVE];
    buf_state_t states[NSTACKS_REMOVE];
    int ranks[NSTACKS_REMOVE];

    // Number of stacks for candidates
    int nstacks_remove = buf->nstacks_remove;

    // Total rest number of available stacks
    int avail_stacks = buf->nproc;
    int curr_nstacks = 0;

    // Number of attempts to lock the mutex
    int nattempts = 0;

    begin_RMA_epoch_all(buf->win);

    // Random choose stacks and get the candidate elements.
    while (curr_nstacks < nstacks_remove) {
        int rank = 0;
        
        // FIXME Optimize this search&found place.
        do {
            rank = get_rand(buf->nproc);
        } while (isfound(rank, ranks, curr_nstacks));

        int rc = buf_tryfetch_elem(&elem_cand[curr_nstacks],
                                   &states[curr_nstacks], buf, rank);

        if (rc == CODE_SUCCESS) {
            ranks[curr_nstacks] = rank;
            curr_nstacks++;
            nattempts = 0;
        } else if (rc == CODE_BUFFER_BUSY) {
            // If lock is busy, try another process.

            // Count number of attempts to lock the mutex
            // to avoid deadlock (only for 2nd and following stacks)
            if (curr_nstacks > 0) {
                // If number of attempts is too large,
                // we suppose it's a deadlock and unlock
                // all locked stacks

                // FIXME not all but some of them?
                if (nattempts > buf->max_attempts) {
                    printf("%d \t DEADLOCK?\n", myrank);

                    int i;
                    for (i = 0; i < curr_nstacks; i++) {
                        mutex_unlock(&buf->lock, buf->win,
                                     buf->lockdisp[ranks[i]], ranks[i]);
                    }
                    curr_nstacks = 0;
                }
            }
            continue;

        } else if (rc == CODE_BUFFER_EMPTY) {
            // printf("%d \t EMPTY rank %d\n", myrank, rank);

            avail_stacks--;
            if (avail_stacks < nstacks_remove) {
                nstacks_remove = avail_stacks;

                if (nstacks_remove == 0) {
                    end_RMA_epoch_all(buf->win);
                    error_msg("buf_pop() failed: buffer is empty", 0);
                    return CODE_BUFFER_EMPTY;
                }
            }
        }
    }

    // Compare candidate elements by timestamp and choose the element with minimal ts.
    double ts_min = elem_cand[0].ts;
    *val = elem_cand[0].val;
    int best_rank = ranks[0];

    int i;
    for (i = 1; i < nstacks_remove; i++) {
        if (elem_cand[i].ts < ts_min) {
            ts_min = elem_cand[i].ts;
            *val = elem_cand[i].val;
            best_rank = ranks[i];
        }
    }

    // printf("%d \t rank %d is the best with ts %f\n", myrank, best_rank, ts_min);

    // Finalize epochs and critical sections
    for (i = 0; i < nstacks_remove; i++) {
        bool remove_flag = (ranks[i] == best_rank);

        buf_get_elem_finalize(states[i], buf, ranks[i], remove_flag);
    }

    end_RMA_epoch_all(buf->win);

    return CODE_SUCCESS;
}

// buf_free: Free memory and so on.
void buf_free(buf_t *buf)
{
    MPI_Barrier(buf->comm);

    MPI_Free_mem(buf->data);
    MPI_Free_mem(buf);
    MPI_Free_mem(buf->basedisp);
    MPI_Free_mem(buf->datadisp);
    MPI_Free_mem(buf->lockdisp);
}

// buf_print: Print buffer.
void buf_print(buf_t *buf, const char *label)
{
    printf("%d \t %s \t ", myrank, label);
    
    int i;
    for (i = 0; i < buf->state.size; i++) {
        printf("%d ", buf->data[i].val);

        if (buf->state.top == i + 1) {
            printf("(t) ");
        }
    }
    printf("\n");
}
