#!/bin/bash
#for ngpu in 1 2 3
#do
#    p=$((ngpu*15))
#    for req in 1 4
#    do 
#        for len in 1 2 4 8
#        do
#            for query in cuda cuda_dp hybrid cpu
#            do
#               cmd="./queryIndex ${p} --ngpu ${ngpu} --nreq ${req} --type ${query} --iter 3 --len ${len} --report"
#				# echo $cmd
#				eval $cmd
#            done
#        done
#    done
#done

for ngpu in 3
do
	for req in 4 6 8
	do 
  		for len in 4 6 8
    	do
			for query in cuda cuda_dp hybrid 
			do
				cmd="./queryIndex sample_45 --ngpu ${ngpu} --nreq ${req} --type ${query} --iter 3 --len ${len} --report"
				eval $cmd
			done
		done
	done
done

#for req in 1 4
#do
#	for len in 1 2 4 8
#	do
#		cmd="./queryIndex full --ngpu 3 --nreq ${req} --type cpup --iter 3 --len ${len} --report"
#		eval $cmd
#	done
#done

#for ngpu in 1 2 3
#do
#   for req in 1 4
#   do 
#       for len in 1 2 4 8
#       do
#           for query in cuda cuda_dp hybrid
#          do
#               cmd="./queryIndex 15 --ngpu ${ngpu} --nreq ${req} --type ${query} --iter 3 --len ${len} --report"
#			# echo $cmd
#              eval $cmd
#          done
#      done
#  done
#done

