#!/bin/bash 


#clear
for i in 0 1 2 3 4 5 6; do
	for j in 4; do 
		export MR_NUMTHREADS=$j
		export MR_L1CACHESIZE=32768
		export MR_NUMPROCS=$j 
		export MR_1QPERTASK=1
		#export MR_KEYMATCHFACTOR=$i
		echo "run $i, procs $j"
		out=`printf "data/procs-pipe-%04d.txt" $j`
		./main input10.dat $i 1>> "$out"
	done
done

