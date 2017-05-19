#!/bin/bash

rsize=16777216
#rsize=268435456
ssize=268435456
prefetchswitch=1
granularity=1
granularityswitchoff=1-$granularity
i=8
j=16
while [ $i -le 129 ];do 
	echo ============================= tuple size is $i ===============================
	echo --- granularity=$granularity AND prefetch=$prefetchswitch---
	g++ -pthread -O2 -DDATASET_TYPE=2 -DMACHINE_TYPE=0 -DPREFETCH_ON=$prefetchswitch -DGRANULARITY_TEST=$granularity -DTUPLE_SIZE=$i -DR_SIZE=$rsize -DS_SIZE=$ssize no_partition_mt.c -o no_partition_mt
#echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 1 thread: no numa, no false share ~~~~~~~~~~~~~~~~~~~~
#	./no_partition_mt 1
#	./no_partition_mt 1
#	./no_partition_mt 1
#	./no_partition_mt 1
#	./no_partition_mt 1
echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 10 threads: no numa, but false share ~~~~~~~~~~~~~~~~~
#        ./no_partition_mt 10
#        ./no_partition_mt 10
#        ./no_partition_mt 10
        ./no_partition_mt 10
        ./no_partition_mt 10
#echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 20 threads: numa and false share ~~~~~~~~~~~~~~~~~~~~~~
#	./no_partition_mt 20
#	./no_partition_mt 20
#	./no_partition_mt 20
#	./no_partition_mt 20
#	./no_partition_mt 20

	rm no_partition_mt
	echo "***************************************"
	i=$((i*2))
done
echo

if false; then

while [ $j -le 129 ];do
        echo ============================= tuple size is $j ===============================
        echo --- granularity AND prefetch---
        g++ -pthread -O2 -DDATASET_TYPE=2 -DMACHINE_TYPE=0 -DPREFETCH_ON=1 -DGRANULARITY_TEST=$granularity -DTUPLE_SIZE=$j -DR_SIZE=$rsize -DS_SIZE=$ssize no_partition_mt.c -o no_partition_mt
echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 1 thread: no numa, no false share ~~~~~~~~~~~~~~~~~~~~
        ./no_partition_mt 1
        ./no_partition_mt 1
        ./no_partition_mt 1
        ./no_partition_mt 1
        ./no_partition_mt 1
echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 10 threads: no numa, but false share ~~~~~~~~~~~~~~~~~
        ./no_partition_mt 10
        ./no_partition_mt 10
        ./no_partition_mt 10
        ./no_partition_mt 10
        ./no_partition_mt 10
echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ 20 threads: numa and false share ~~~~~~~~~~~~~~~~~~~~~~
        ./no_partition_mt 20
        ./no_partition_mt 20
        ./no_partition_mt 20
        ./no_partition_mt 20
        ./no_partition_mt 20

        rm no_partition_mt
        echo "***************************************"
        j=$((j*2))
done
echo
fi

if false; then

while [ $j -le 129 ];do
        echo ============================= tuple size is $j ===============================
        echo --- granularity_or_not ---
        g++ -pthread -O2 -DDATASET_TYPE=2 -DMACHINE_TYPE=0 -DPREFETCH_ON=$prefetchswitch -DGRANULARITY_TEST=$granularityswitchoff -DTUPLE_SIZE=$j -DR_SIZE=$rsize -DS_SIZE=$ssize no_partition_mt.c -o no_partition_mt

        ./no_partition_mt 40
        ./no_partition_mt 40
        ./no_partition_mt 40
        ./no_partition_mt 40
        ./no_partition_mt 40
        rm no_partition_mt
        echo "***************************************"
        j=$((j*2))
done
echo
fi
