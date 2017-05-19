#!/bin/bash

rsize=16777216
ssize=268435456
prefetchswitch=1
granularity=1
prefetchdistance=4

while [ $prefetchdistance -le 20 ];do 
	echo ============================= the prefetch distance is $prefetchdistance ===============================
	g++ -pthread -O2 -DDATASET_TYPE=2 -DMACHINE_TYPE=0 -DPREFETCH_ON=$prefetchswitch -DPREFETCH_DISTANCE=$prefetchdistance -DGRANULARITY_TEST=$granularity -DTUPLE_SIZE=16 -DR_SIZE=$rsize -DS_SIZE=$ssize no_partition_mt.c -o no_partition_mt

	./no_partition_mt 20
	./no_partition_mt 20
	rm no_partition_mt
	echo "***************************************"
	prefetchdistance=$((prefetchdistance+2))
done
