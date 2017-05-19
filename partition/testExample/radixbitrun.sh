#!/bin/sh
i=8
while [ $i -le 12 ]; do
	echo "================================================"
	echo "Radix Bit = $i"
	# For Power8 machine_type=1; For Intel use machine_type=0
	# For random data DATASET_TYPE=1; For sequential data DATASET_TYPE=0; For others DATASET_TYPE=2
	g++ -pthread -O2 -DTUPLE_SIZE=128 -DDATASET_TYPE=2 -DNUM_PASSES=2 -DMACHINE_TYPE=0 -DNUM_RADIX_BITS=$i -DTIME_OF_PHASE=1 partition.c -o partition

	echo "-------------------------------------------"
echo 10 threads
	./partition 10
	echo "-------------------------------------------"
	./partition 10
	echo "-------------------------------------------"
	./partition 10
	echo "-------------------------------------------"
	./partition 10
	echo "-------------------------------------------"
	./partition 10
	echo "-------------------------------------------"
echo
echo 20 threads
	./partition 20
	echo "-------------------------------------------"
	./partition 20
	echo "-------------------------------------------"
	./partition 20
	echo "-------------------------------------------"
	./partition 20
	echo "-------------------------------------------"
	./partition 20
	echo "-------------------------------------------"
	i=$((i+1))
	echo
	rm partition
done
