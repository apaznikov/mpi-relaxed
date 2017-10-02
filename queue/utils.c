/*
 * utils.c: Utils for MPI relaxed data structures
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#include <mpi.h>
#include <stdio.h>
#include <stdbool.h>

#include "utils.h"
#include "mpigclock.h"

extern int myrank, nproc;

/* issynchronized: Check is MPI_Wtime is synchronized. */
static bool issynchronized(MPI_Comm comm) 
{
    void *attr_val_ptr;
    int attr_val;
    int flag;

    MPI_Comm_get_attr(comm, MPI_WTIME_IS_GLOBAL, &attr_val_ptr, &flag);
    
    if (flag) {
        attr_val = *(int*) attr_val_ptr;
        return attr_val != 0;
    } else {
        printf("Error: cannot read the value of \"MPI_WTIME_IS_GLOBAL\"\n");
        return false;
    }
}

/* mpi_sync_time: Syncrhonize time over all MPI processes. 
 * Return offset of current process from 0 process. */
double mpi_sync_time(MPI_Comm comm)
{
    if (issynchronized(comm)) {
        /* It's ok, MPI_Wtime is synchronized */
        return 0.;
    } else {
        /* We need to synchronize time */
        double rtt;
        return mpigclock_sync_log(comm, 0, &rtt);
    }
}
