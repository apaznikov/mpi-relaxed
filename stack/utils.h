/*
 * utils.h: Relaxed distributed stack implementation on MPI
 * 
 * (C) 2017 Alexey Paznikov <apaznikov@gmail.com>
 * (C) 2019 Aleksandr Polozhenskii <polozhenskii@gmail.com>
 *
 */

#pragma once

/* mpi_sync_time: Synchronize time over all MPI processes.
 * Return offset of current process from 0 process. */
double mpi_sync_time(MPI_Comm comm);

/* init_random_generator: Initialize random generator */
void init_random_generator(void);

/* get_rand: Get random value from 0 to (maxval - 1) */
int get_rand(int maxval);