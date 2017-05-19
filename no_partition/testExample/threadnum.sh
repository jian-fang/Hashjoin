#!/bin/bash

rsize=16777216
ssize=268435456
#ssize=134217728
prefetchswitch=1
granularity=1
tuplesize=16

g++ -pthread -O2 -DDATASET_TYPE=2 -DMACHINE_TYPE=0 -DPREFETCH_ON=$prefetchswitch -DGRANULARITY_TEST=$granularity -DTUPLE_SIZE=$tuplesize -DBUCKET_SIZE=2 -DR_SIZE=$rsize -DS_SIZE=$ssize no_partition_mt.c -o no_partition_mt

i=10

while [ $i -le 20 ];do
echo "------------------------------   $i threads   -----------------------------"
#perf stat -e context-switches,cycles,instructions,stalled-cycles-frontend,cpu-migrations,page-faults,task-clock,cache-misses,LLC-load-misses,LLC-store-misses ./no_partition_mt $i
./no_partition_mt $i
./no_partition_mt $i
./no_partition_mt $i
./no_partition_mt $i
./no_partition_mt $i
i=$((i+1))
done
