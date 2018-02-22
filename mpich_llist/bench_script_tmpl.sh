#!/bin/sh 

NRUNS=%%NRUNS%%
NUM_ELEMS=%%NUM_ELEMS%%
EXEC=%%BENCH%%
NPROC=%%NPROC%%

timesum=0

for ((run = 1; run <= NRUNS; run++)); do
    /usr/bin/time -f %e -o /tmp/runtime mpiexec ./$EXEC
    runtime=`cat /tmp/runtime`
    echo runtime $run $runtime
    timesum=`echo $timesum $runtime | awk '{ sum = $1 + $2 } END { print sum }'`
    echo timesum $timesum
done

echo timesum $timesum

avgtime=`echo $timesum $NRUNS | awk '{ print $1 / $2 }'`
throughput=`echo $NUM_ELEMS $avgtime | awk '{ print $1 / $2 }'`

echo -e "$NPROC \t $throughput" >>../results/$EXEC.dat
