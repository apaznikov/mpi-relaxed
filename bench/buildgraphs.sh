#!/bin/sh

function process_output
{
    [ ! -d "graphs" ] && mkdir graphs

    echo -e "nproc \t time" >graphs/time
    echo -e "nproc \t throughput" >graphs/throughput

    outputs=`ls jobfiles/*.out`
    for out in $outputs; do
        nproc=`cat $out | egrep "Numer of processes" | awk '{print $4}'`
        etime=`cat $out | egrep "Elapsed time" | awk '{print $3}'`
        throughput=`cat $out | egrep "Throughput" | awk '{print $2}'`
        echo -e "$nproc \t $etime" >>graphs/time
        echo -e "$nproc \t $throughput" >>graphs/throughput
    done
}

function build
{
    name="$1"

    gp=graphs/${name}.gp
    cat tmpl.gp | sed "s/%%NAME%%/$name/g" >$gp
    gnuplot $gp
}

process_output
build throughput
