# 
# benchmark.sh: Simple benchmark script for RCL
# (C) 2015-2016 Alexey Paznikov <apaznikov@gmail.com> 
# 

#!/bin/sh

BENCHNAME=../queue/bench
TASKJOB_TMPL=../queue/task.job.tmpl
EXEC="\.\.\/\.\.\/queue\/bench"

NNODES=18
PPN=8

function measure_throughput
{
    throughput=`$@ | egrep "Throughput" | awk '{print $3}'`
    echo $throughput >>results.tmp
}

function measure_avg
{
    cat /dev/null >results.tmp

    for ((i = 1; i <= NRUNS; i++)); do
        cmd="./$BENCHNAME"
        echo $cmd
        measure_throughput $cmd
    done

    avg=`awk '{ total += $1; count++ } END { print total / count }' results.tmp`

    echo -e "$dat_first_col \t\t $avg" >>$dat

    rm results.tmp
}

function prepare_dirs
{
    # [ ! -d "results" ] && mkdir results
    [ -d "jobfiles" ] && rm -r jobfiles 
    mkdir jobfiles
}

#
# THROUGHPUT test
#
function scalability
{
    for memaffin in $MEMAFFIN; do
        for access_pattern in $ACCESS_PATTERN; do
            datfile="results/${memaffin}_${access_pattern}_scalability.dat"
            echo -e "nthreads \t throughput, op/s" >$datfile

            for nthreads in $NTHREADS_HW; do
                affinity_file="thread_affinity_${nthreads}threads"

                measure 
            done
        done
    done
}

function prepare_jobfiles
{
    for ((nnodes = 1; nnodes <= NNODES; nnodes++)); do
        cat $TASKJOB_TMPL | sed s/%NODES%/$nnodes/g \
                          | sed s/%PPN%/$PPN/g \
                          | sed s/%EXEC%/$EXEC/g \
                          >jobfiles/nodes$nnodes-ppn$ppn-run$run.job
    done
}

function runjobs
{
    jobfiles_arr=( $(ls jobfiles) )
    njobs=${#jobfiles_arr[@]}

    cd jobfiles

    for ((i = 0; i < $njobs; i++)); do
        cmd="qsub ${jobfiles_arr[$i]}"
        echo $cmd
        eval $cmd
    done
}

prepare_dirs
prepare_jobfiles
runjobs
