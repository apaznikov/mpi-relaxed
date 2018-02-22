#!/bin/sh

BENCH_LIST="linked_list_bench_lock_all \
            linked_list_bench_lock_excl \
            linked_list_bench_lock_shr \
            linked_list \
            linked_list_fop \
            linked_list_lockall"

for bench in $BENCH_LIST; do
    dat="results/${bench}.dat"
    cat $dat | sort -n >$dat.tmp
    mv $dat.tmp $dat
done

gnuplot linked_list.gp
