/*
 * relaxed_stack.h: Relaxed distributed stack implementation on MPI
 * 
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com>
 * (C) 2019 Aleksandr Polozhenskii <polozhenskii@gmail.com>
 *
 */

#pragma once

#include <mpi.h>

enum {
    BUFFER_STARTSIZE      = 250000,
    // BUFFER_STARTSIZE      = 1000,
    NSTACKS_REMOVE        = 2,

    CODE_SUCCESS          = 0,
    CODE_ERROR            = 1,
    CODE_BUFFER_FULL      = 2,
    CODE_BUFFER_EMPTY     = 3,
    CODE_BUFFER_BUSY      = 4,

    CODE_TRYLOCK_SUCCESS  = 0,
    CODE_TRYLOCK_BUSY     = 1,

    LOCK_UNLOCKED         = 0,
    LOCK_LOCKED           = 1,
    MIN_DELAY             = 10,
    MAX_DELAY             = 1000     
};

typedef int val_t;

typedef struct {
    val_t val;
    double ts;
} elem_t;

typedef struct {
    int top;
    int size;
} buf_state_t;

typedef struct {
    int state;
    int unlocked;
    int locked;
    int result;
} lock_t;

// Buffer
typedef struct {
    MPI_Aint basedisp_local;
    MPI_Aint *basedisp;
    MPI_Aint *lockdisp;
    elem_t *data;
    MPI_Aint datadisp_local;
    MPI_Aint *datadisp;
    buf_state_t state;
    lock_t lock;
    MPI_Win win;
    MPI_Comm comm;
    int nproc;
    double ts_offset;
    int nstacks_remove;
    int max_attempts;
} buf_t;

// Process-oblivious buffer info
typedef struct {
    size_t lock_state_offset;
    size_t state_offset;
    size_t top_offset;
    size_t buf_offset;
} buf_info_t;

// buf_init: Init buffer with specified size.
int buf_init(buf_t **buf, int size, MPI_Comm comm);

// buf_free: Free memory and so on.
void buf_free(buf_t *buf);

// buf_push: Choose randomly the stack and push element into it.
int buf_push(val_t val, buf_t *buf);

// buf_pop: 
int buf_pop(val_t *val, buf_t *buf);

// buf_push_proc: Push an element on specified process.
int buf_push_proc(elem_t elem, buf_t *buf, int rank);

// buf_pop_proc: Remove an element from the buffer on specified process.
int buf_pop_proc(elem_t *elem, buf_t *buf, int rank);

// buf_print: Print buffer.
void buf_print(buf_t *buf, const char *label);

// error_msg: Print error message.
void error_msg(const char *msg, int _errno);
