#!/bin/bash

#PBS -N mpitask
#PBS -l walltime=00:10:00
#PBS -l nodes=2:ppn=1
#PBS -m n
#PBS -j oe
#PBS -o stdout

cd $PBS_O_WORKDIR

mpiexec ./bench
