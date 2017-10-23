/*
 * utils.h: Utils for MPI relaxed data structures
 *
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com> 
 */

#pragma once

#include "common.h"

/* mpi_sync_time: Syncrhonize time over all MPI processes. 
 * Return offset of current process from 0 process. */
double mpi_sync_time(MPI_Comm comm);

/* init_random_generator: Initialize random generator. */
void init_random_generator(void);

/* get_rand: Get random value from 0 to (maxval - 1). */
int get_rand(int maxval);
