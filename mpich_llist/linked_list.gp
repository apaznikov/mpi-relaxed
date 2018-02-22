#set term pngcairo transparent enhanced font "Times,26" size 1200,800
set term pngcairo enhanced font "Times,24" size 1200,800
#set xlabel "%%XLABEL%%" 
#set ylabel "%%YLABEL%%" 
set output "results/lists.png"
set key inside top left nobox
#set key outside bmargin nobox
#set nokey

set border lw 3
set grid lw 2.5
set pointsize 3.0
set xtics 8

plot "results/linked_list_bench_lock_excl.dat" using 1:2 \
     ti "linked list bench lock excl" \
     with lp dt "_" lw 4 pt 7 lc rgb '#007BCC', \
     \
     "results/linked_list_bench_lock_shr.dat" using 1:2 \
     ti "linked list bench lock shr" \
     with lp dt "_.." lw 4 pt 5 lc rgb '#007BCC', \
     \
     "results/linked_list.dat" using 1:2 \
     ti "linked list" \
     with lp dt 1 lw 4 pt 2 lc rgb '#C40D28', \
     \
     "results/linked_list_fop.dat" using 1:2 \
     ti "linked list fop" \
     with lp dt "_" lw 4 pt 7 lc rgb '#C40D28', \
     \
     "results/linked_list_lockall.dat" using 1:2 \
     ti "linked list lockall" \
     with lp dt "_.." lw 4 pt 5 lc rgb '#C40D28'
#"results/linked_list_bench_lock_all.dat" using 1:2 \
#     ti "linked list bench lock all" \
#     with lp dt 1 lw 4 pt 2 lc rgb '#007BCC', \
#     \
