#!/bin/sh

g++ -pthread partition.c -o partition

i=1
while [ $i -le 33 ]; do
	echo "$i threads:"
	./partition $i
	i=$((i*2))
done
