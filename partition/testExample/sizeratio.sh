#!/bin/sh

rsize=16777216
#ssize=268435456
ssize=16777216
end=$((ssize*4))
tuplesize=16

while [ $rsize -le $end ]; do
	echo "================================================"
	echo "R Talbe Size = $rsize"
	# For Power8 machine_type=1; For Intel use machine_type=0
	# For random data DATASET_TYPE=1; For sequential data DATASET_TYPE=0; For others DATASET_TYPE=2
	g++ -pthread -O2 -DTUPLE_SIZE=$tuplesize -DDATASET_TYPE=2 -DNUM_PASSES=2 -DR_SIZE=$rsize -DS_SIZE=$ssize -DMACHINE_TYPE=0 -DNUM_RADIX_BITS=10 -DTIME_OF_PHASE=1 partition.c -o partition

	echo "-------------------------------------------"
echo 10 threads
	./partition 10
	echo "-------------------------------------------"
	./partition 10
	echo "-------------------------------------------"
	./partition 10
#	echo "-------------------------------------------"
#	./partition 10
#	echo "-------------------------------------------"
#	./partition 10
#	echo "-------------------------------------------"
#	./partition 10
#	echo "-------------------------------------------"
#	./partition 10
#	echo "-------------------------------------------"
#echo
#echo 20 threads
#	./partition 20
#	echo "-------------------------------------------"
#	./partition 20
#	echo "-------------------------------------------"
#	./partition 20
#	echo "-------------------------------------------"
#	./partition 20
#	echo "-------------------------------------------"
#	./partition 20
#	echo "-------------------------------------------"
	rsize=$((rsize*4))
	echo
	rm partition
done
