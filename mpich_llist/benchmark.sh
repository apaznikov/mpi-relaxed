NRUNS=2
# NUM_ELEMS=1500
NUM_ELEMS=100000
NNODES=10
PPN=8

# BENCH_LIST="linked_list_bench_lock_all \
#             linked_list_bench_lock_excl \
#             linked_list_bench_lock_shr \
#             linked_list \
#             linked_list_fop \
#             linked_list_lockall"

BENCH_LIST="linked_list_bench_lock_excl \
            linked_list_bench_lock_shr"
            # linked_list \
            # linked_list_fop"

# BENCH_LIST="linked_list_bench_lock_all"

[ -d "bin" ] && rm -r bin 
mkdir bin 
[ ! -d "results" ] && mkdir results

function compile
{
    cat "src/mpitest.h.in" | sed "s/%%NUM_ELEMS%%/$NUM_ELEMS/g" \
                             >src/mpitest.h

    make
}

function prepare_jobs
{
    for bench in $BENCH_LIST; do
        for ((nnodes = 1; nnodes <= NNODES; nnodes++)); do
            nproc=`echo $nnodes $PPN | awk '{ print $1 * $2 }'`
            name="bin/${bench}-nodes${nnodes}-ppn${PPN}"

            cat bench_script_tmpl.sh | sed "s/%%NRUNS%%/$NRUNS/g" \
                                     | sed "s/%%NUM_ELEMS%%/$NUM_ELEMS/g" \
                                     | sed "s/%%BENCH%%/$bench/g" \
                                     | sed "s/%%NPROC%%/$nproc/g" \
                                       >${name}.sh

            chmod +x ${name}.sh

            cat torque_task_tmpl.job | sed "s/%%NODES%%/$nnodes/g" \
                                     | sed "s/%%PPN%%/$PPN/g" \
                                     | sed "s/%%BENCH%%/$bench/g" \
                                     >${name}.job
        done

        cat /dev/null >results/$bench.dat
    done
}

function runjobs
{
    cd bin
    jobfiles=`ls *.job`

    for job in $jobfiles; do
       cmd="qsub $job"
       echo $cmd
       eval $cmd
    done
}

compile
prepare_jobs
runjobs
