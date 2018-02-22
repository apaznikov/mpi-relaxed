#set term pngcairo transparent enhanced font "Times,26" size 1200,800
set term pngcairo enhanced font "Times,24" size 1200,800
#set xlabel "%%XLABEL%%" 
#set ylabel "%%YLABEL%%" 
set output "graphs/%%NAME%%.png"
set key inside top left nobox
#set key outside bmargin nobox
#set nokey

set border lw 3
set grid lw 2.5
set pointsize 3.0

plot "graphs/throughput" using 1:2 \
     ti "%%NAME%% - default" \
     with lp lw 4 pt 2 lc rgb '#007BCC'

#     \
#     "results.numa/%%NAME%%_%%TYPE%%.dat" using 1:2 \
#     ti "%%NAME%% - RCLLockInitNUMA" \
#     with lp dt "_" lw 4 pt 7 lc rgb '#007BCC', \
#     \
#     "results.affinity/%%NAME%%_%%TYPE%%.dat" using 1:2 \
#     ti "%%NAME%% - RCLHierarchicalAffinity" \
#     with lp dt "_.." lw 4 pt 5 lc rgb '#007BCC', \
#     \
#     "results.numa_affinity/%%NAME%%_%%TYPE%%.dat" using 1:2 \
#     ti "%%NAME%% - RCLLockInitNUMA and RCLHierarchicalAffinity" \
#     with lp dt 1 lw 4 pt 2 lc rgb '#C40D28'
#     \
#     "results/rcl_server_node_seq_%%NAME%%.dat" using 1:2 \
#     ti "RCL server node, sequentional" \
#     with lp dt "_" lw 4 pt 7 lc rgb '#C40D28', \
#     \
#     "results/rcl_server_node_strided_%%NAME%%.dat" using 1:2 \
#     ti "RCL server node, strided" \
#     with lp dt "_.." lw 4 pt 5 lc rgb '#C40D28'
