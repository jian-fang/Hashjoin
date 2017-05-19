// **************************************************************************//
// Running on defferent machine, you need to change the following parameters //
// CACHE_LINE_SIZE                                                           //
// ALIGNED_SIZE                                                              //
// **************************************************************************//
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include "lock.h"

// 0 for Intel
// 1 for Power8
#ifndef MACHINE_TYPE
#define MACHINE_TYPE 0
#endif

// 0 for Intel
// 1 for Power8
#ifndef CPU_MAPPING_TYPE
#define CPU_MAPPING_TYPE 0
#endif

#ifndef TUPLE_SIZE
#define TUPLE_SIZE 16
#endif

#ifndef R_SIZE
#define R_SIZE (1 << 24)
#endif

#ifndef S_SIZE
#define S_SIZE (1 << 28)
#endif

// DATASET_TYPE==1 ==> random data
// DATASET_TYPE==0 ==> unique data
// DATASET_TYPE==else ==> repeat within 16M
#ifndef DATASET_TYPE
#define DATASET_TYPE 0
#endif

#ifndef BUCKET_SIZE
#define BUCKET_SIZE 2
#endif

#ifndef OVERFLOW_BUF_SIZE
#define OVERFLOW_BUF_SIZE 1024
#endif

#if MACHINE_TYPE==1
#define CACHE_LINE_SIZE 128	// 128 for Power, and 64 for Intel
#elif MACHINE_TYPE==0
#define CACHE_LINE_SIZE 64
#else
#define CACHE_LINE_SIZE 64
#endif

#ifndef ALIGNED_SIZE
#define ALIGNED_SIZE 64
#endif

#ifndef PREFETCH_ON
#define PREFETCH_ON 0
#endif

#if PREFETCH_ON==1
#ifndef PREFETCH_DISTANCE
#define PREFETCH_DISTANCE 10
#endif
#endif

#ifndef GRANULARITY_TEST
#define GRANULARITY_TEST 0
#endif

typedef int64_t intkey_t;
typedef int64_t intvalue_t;
typedef int64_t intnum_t;
typedef struct arg_t arg_t;
typedef struct tuple_t tuple_t;
typedef struct relation_t relation_t;
typedef struct bucket_buffer_t bucket_buffer_t;
typedef struct hashtable_t hashtable_t;

// To find the nearest number with power of 2.
// For this define, maximum is 2^32.
// Add V |= V >> 32 at the end to make the maximum be 2^64
#ifndef NEXT_POWER_TWO
#define NEXT_POWER_TWO(V)		\
	do				\
	{				\
		V--;			\
		V |= V >> 1;		\
		V |= V >> 2;		\
		V |= V >> 4;		\
		V |= V >> 8;		\
		V |= V >> 16;		\
		V |= V >> 32;		\
		V++;						\
	}while(0)
#endif

// Wait for the barrier
#ifndef BARRIER_ARRIVE
#define BARRIER_ARRIVE(B,RV)				\
	RV = pthread_barrier_wait(B);			\
	if(RV!=0 && RV!=PTHREAD_BARRIER_SERIAL_THREAD)	\
	{						\
		printf("wait for barrier errer!\n");	\
		exit(EXIT_FAILURE);			\
	}
#endif

// Define the hash function
// Use the num_buckets as a seed
// and the remainder as the hash value
#ifndef HASH
#define HASH(W,SEED)			\
	((W)&(SEED))
#endif

// ------------------------------------- Data structure ----------------------------------//
struct tuple_t
{
	intkey_t key;
#if TUPLE_SIZE==16 || TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
	intvalue_t value;
#if TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
	int64_t value1;
	int64_t value2;
#if TUPLE_SIZE==64 || TUPLE_SIZE==128
	int64_t value3;
	int64_t value4;
	int64_t value5;
	int64_t value6;
#if TUPLE_SIZE==128
	int64_t value7;
	int64_t value8;
	int64_t value9;
	int64_t value10;
	int64_t value11;
	int64_t value12;
	int64_t value13;
	int64_t value14;
#endif
#endif
#endif
#endif
};

struct relation_t
{
	tuple_t * tuples;
	intnum_t num_tuples;
};

struct bucket_t
{
	tuple_t tuples[BUCKET_SIZE];
	struct bucket_t* next;
	int32_t count;
	volatile char latch; // for the lock
}__attribute__((aligned(ALIGNED_SIZE)));

struct hashtable_t
{
	bucket_t* buckets;
	intnum_t num_buckets;
};

struct bucket_buffer_t
{
	struct bucket_buffer_t* next;
	intnum_t count;
	bucket_t buf[OVERFLOW_BUF_SIZE];
};

struct arg_t
{
	int32_t tid;			// thread ID
	hashtable_t* ht;		// hash table
	relation_t relR;		// two relations that need to do the join
	relation_t relS;
	pthread_barrier_t* barrier;	// pthread barrier
	intnum_t num_results;		// number of the join result
};

// ------------------------------------- Functions ----------------------------------//

// Function: 		alloc_aligned
// Functionality:	allocate new space
// Input:		space size to be allocated
// Output:		a pointer points to this space
void * alloc_aligned(size_t size,size_t aligned_size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, aligned_size, size);

    if (rv) {
        perror("[ERROR] alloc_aligned() failed: out of memory");
        return 0;
    }

    return ret;
};

// Function: 		initial_bucket_buffer
// Functionality:	allocate the first bucket_buffer_t
// Input:		void
// Output:		a bucket_buffer_t pointer points to this bucket_buffer_t
bucket_buffer_t* initial_bucket_buffer()
{
	bucket_buffer_t* overflowbuf;
	overflowbuf = (bucket_buffer_t*) malloc(sizeof(bucket_buffer_t));
	overflowbuf->count = 0;
	overflowbuf->next = NULL;

	return overflowbuf;
}

// Function: 		free_bucket_buffer
// Functionality:	free the space of the bucket_buffer_t
// Input:		pointer of the bucket_buffer_t
// Output:		void
void free_bucket_buffer(bucket_buffer_t* buf)
{
	do
	{
		bucket_buffer_t* temp = buf->next;
		free(buf);
		buf = temp;
	}while(buf);
}

// Function: 		get_new_bucket
// Functionality:	get new bucket from the bucket_buffer_t
// Input:		pointer of the pointer that points to the old bucket_buffer_t
//			pointer of the pointer that points to the new bucket
// Output:		void
// usage:		get_new_bucket(&bucket,&overflowbuf)
// comment:		this function is an inline function
static inline void get_new_bucket(bucket_t ** result, bucket_buffer_t ** buf)
{
	if ((*buf)->count < OVERFLOW_BUF_SIZE)
	{
		*result = (*buf)->buf + (*buf)->count;
		(*buf)->count ++;
	}
	else
	{
		bucket_buffer_t * new_buf = (bucket_buffer_t*) malloc(sizeof(bucket_buffer_t));
		new_buf->count = 1;
		new_buf->next = *buf;
		*buf = new_buf;
		*result = new_buf->buf;
	}
}

void shuffle_relation(relation_t* rel)
{
        int i;
        for(i=rel->num_tuples-1;i>0;i--)
        {
                intkey_t j = rand()%i;
                intkey_t tmp = rel->tuples[i].key;
                rel->tuples[i].key = rel->tuples[j].key;
                rel->tuples[j].key = tmp;
        }
}

// Function: 		initial_relation
// Functionality:	initial the relations
// Input:		the number of the tuples in the relations
// Output:		a relation_t pointer that points to the relations
relation_t * initial_relation(intnum_t num_tuples)
{
	relation_t * rel = (relation_t*)calloc(1,sizeof(relation_t));
	if(!rel)
	{
		perror("allocate relation error\n");
		exit(1);
	}

	rel->num_tuples = num_tuples;

	rel->tuples = (tuple_t*) alloc_aligned(rel->num_tuples * sizeof(tuple_t),CACHE_LINE_SIZE);
	if (!rel->tuples)
	 {
		perror("out of memory");
		exit(1);
	}

	for (intnum_t i=0;i<num_tuples;i++)
	{
#if DATASET_TYPE==1
		rel->tuples[i].key = rand()%(R_SIZE);
#elif DATASET_TYPE==0
		rel->tuples[i].key = i+1;
#else
		rel->tuples[i].key = (i+1)%(R_SIZE);
#endif
	}

	shuffle_relation(rel);

	for (intnum_t i=0;i<num_tuples;i++)
	{
#if TUPLE_SIZE==16 || TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
                rel->tuples[i].value = rel->tuples[i].key;
#if TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
                rel->tuples[i].value1 = rel->tuples[i].key;
                rel->tuples[i].value2 = rel->tuples[i].key;
#if TUPLE_SIZE==64 || TUPLE_SIZE==128
                rel->tuples[i].value3 = rel->tuples[i].key;
                rel->tuples[i].value4 = rel->tuples[i].key;
                rel->tuples[i].value5 = rel->tuples[i].key;
                rel->tuples[i].value6 = rel->tuples[i].key;
#if TUPLE_SIZE==128
                rel->tuples[i].value7 = rel->tuples[i].key;
                rel->tuples[i].value8 = rel->tuples[i].key;
                rel->tuples[i].value9 = rel->tuples[i].key;
                rel->tuples[i].value10 = rel->tuples[i].key;
                rel->tuples[i].value11 = rel->tuples[i].key;
                rel->tuples[i].value12 = rel->tuples[i].key;
                rel->tuples[i].value13 = rel->tuples[i].key;
                rel->tuples[i].value14 = rel->tuples[i].key;
#endif
#endif
#endif
#endif
	}
	printf("Allocate relation done!\tSize:%ld\n",rel->num_tuples);

	return rel;
}

// Function: 		initial_hashtable
// Functionality:	initial the hash table
// Input:		the number of buckets
// Output:		a hashtable_t pointer that points to the hashtable
hashtable_t* initial_hashtable(intnum_t num_buckets)
{
	hashtable_t * ht;

	ht = (hashtable_t*) malloc(sizeof(hashtable_t));
	ht->num_buckets = num_buckets;
	ht->buckets = (bucket_t*) alloc_aligned(ht->num_buckets*sizeof(bucket_t),CACHE_LINE_SIZE);
	if(!ht->buckets)
	{
		perror("out of memory");
		exit(1);
	}
	memset(ht->buckets,0,ht->num_buckets*sizeof(bucket_t));

	return ht;
}

// Function: 		free_hashtable
// Functionality:	free the space of the hash table
// Input:		the pointer of the hash table
// Output:		void
void free_hashtable(hashtable_t* ht)
{
	free(ht->buckets);
	free(ht);
}
// Function: 		build_relation
// Functionality:	build the hashtable
// Input:		the relation that need to build the hash table
//			the hash table that need to store the output (the hash table)
// Output:		void
void build_hashtable(relation_t* rel,hashtable_t* ht,bucket_buffer_t** overflowbuf)
{
#if PREFETCH_ON==1
	intnum_t prefetch_index = PREFETCH_DISTANCE;
#endif
	intnum_t mask = ht->num_buckets - 1;

	for(intnum_t i=0;i<rel->num_tuples;i++)
	{
		tuple_t* dest;
		bucket_t* current;
		bucket_t* next;

#if PREFETCH_ON==1
		// prefetch
		if(prefetch_index < rel->num_tuples)
		{
			intnum_t prefetch_bucket_index = HASH(rel->tuples[prefetch_index++].key,mask);
			__builtin_prefetch(ht->buckets + prefetch_bucket_index, 1, 1); // prefetch for write
		}
#endif
		intnum_t index = HASH(rel->tuples[i].key,mask);
		current = ht->buckets + index;
		lock(&current->latch);	// add lock
		next = current->next;

		if(current->count == BUCKET_SIZE)
		{
			if(!next || next->count==BUCKET_SIZE)
			{
				// bucket_t *temp = (bucket_t*) calloc(1,sizeof(bucket_t));
				// use get_new_bucket instead of the above to use pre-allocate
				bucket_t * temp;
				get_new_bucket(&temp,overflowbuf);
				current->next = temp;
				temp->next = next;
				temp->count = 1;
				dest = temp->tuples;
			}
			else
			{
				dest = next->tuples + next->count;
				next->count++;
			}
		}
		else
		{
			dest = current->tuples + current->count;
			current->count++;
		}
		*dest = rel->tuples[i];
		unlock(&current->latch);
	}
}

// Function: 		prboe_relation
// Functionality:	probe the hashtable
// Input:		the relation that need to probe the hash table
//			the hash table that need to be probed
// Output:		void
intnum_t probe_hashtable(relation_t* rel,hashtable_t* ht)
{
	intnum_t num_results = 0;
#if PREFETCH_ON==1
	intnum_t prefetch_index = PREFETCH_DISTANCE;
#endif
	intnum_t mask = ht->num_buckets - 1;

	for(intnum_t i=0;i<rel->num_tuples;i++)
	{
#if PREFETCH_ON==1
		if(prefetch_index<rel->num_tuples)
		{
			intnum_t prefetch_bucket_index = HASH(rel->tuples[prefetch_index++].key,mask);
			__builtin_prefetch(ht->buckets + prefetch_bucket_index, 0, 1);
#if GRANULARITY_TEST==1

#if TUPLE_SZIE==128
			__builtin_prefetch(&(((bucket_t*)(ht->buckets + prefetch_bucket_index))->tuples[7]), 0, 1);
#endif
#if TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
			__builtin_prefetch(&(((bucket_t*)(ht->buckets + prefetch_bucket_index))->next), 0, 1);
#endif

#endif
		}
#endif

		intnum_t index = HASH(rel->tuples[i].key,mask);
		bucket_t* current = ht->buckets + index;

#if GRANULARITY_TEST==1
#if TUPLE_SIZE==16
		int64_t rel_value = rel->tuples[i].value;
#elif TUPLE_SIZE==32
		int64_t rel_value = rel->tuples[i].value;
		rel_value = rel->tuples[i].value1;
                rel_value = rel->tuples[i].value2;
#elif TUPLE_SIZE==64
		int64_t rel_value = rel->tuples[i].value;
		rel_value = rel->tuples[i].value1;
		rel_value = rel->tuples[i].value2;
		rel_value = rel->tuples[i].value3;
		rel_value = rel->tuples[i].value4;
		rel_value = rel->tuples[i].value5;
                rel_value = rel->tuples[i].value6;
#elif TUPLE_SIZE==128
		int64_t rel_value = rel->tuples[i].value;
		rel_value = rel->tuples[i].value1;
		rel_value = rel->tuples[i].value2;
		rel_value = rel->tuples[i].value3;
		rel_value = rel->tuples[i].value4;
		rel_value = rel->tuples[i].value5;
		rel_value = rel->tuples[i].value6;
		rel_value = rel->tuples[i].value7;
		rel_value = rel->tuples[i].value8;
		rel_value = rel->tuples[i].value9;
		rel_value = rel->tuples[i].value10;
		rel_value = rel->tuples[i].value11;
		rel_value = rel->tuples[i].value12;
		rel_value = rel->tuples[i].value13;
		rel_value = rel->tuples[i].value14;
#endif
#endif

		while(current)
		{
			for (int32_t j=0; j<current->count; j++)
			{
#if GRANULARITY_TEST==1
#if TUPLE_SIZE==16
				int64_t hashtable_value = current->tuples[j].value;
#elif TUPLE_SIZE==32
				int64_t hashtable_value = current->tuples[j].value;
				hashtable_value = current->tuples[j].value1;
				hashtable_value = current->tuples[j].value2;
#elif TUPLE_SIZE==64
				int64_t hashtable_value = current->tuples[j].value;
				hashtable_value = current->tuples[j].value1;
				hashtable_value = current->tuples[j].value2;
				hashtable_value = current->tuples[j].value3;
				hashtable_value = current->tuples[j].value4;
				hashtable_value = current->tuples[j].value5;
				hashtable_value = current->tuples[j].value6;
#elif TUPLE_SIZE==128
				int64_t hashtable_value = current->tuples[j].value;
				hashtable_value = current->tuples[j].value1;
				hashtable_value = current->tuples[j].value2;
				hashtable_value = current->tuples[j].value3;
				hashtable_value = current->tuples[j].value4;
				hashtable_value = current->tuples[j].value5;
				hashtable_value = current->tuples[j].value6;
				hashtable_value = current->tuples[j].value7;
				hashtable_value = current->tuples[j].value8;
				hashtable_value = current->tuples[j].value9;
				hashtable_value = current->tuples[j].value10;
				hashtable_value = current->tuples[j].value11;
				hashtable_value = current->tuples[j].value12;
				hashtable_value = current->tuples[j].value13;
				hashtable_value = current->tuples[j].value14;
#endif

#if TUPLE_SIZE==16 || TUPLE_SIZE==32 || TUPLE_SIZE==64 || TUPLE_SIZE==128
				if(rel->tuples[i].key == current->tuples[j].key && rel_value == hashtable_value)
#elif TUPLE_SIZE==8// Where Tuple size = 8, there is no value
				if(rel->tuples[i].key == current->tuples[j].key)
#endif

#else
				if(rel->tuples[i].key == current->tuples[j].key)
#endif
				{
					num_results++;
				}
			}
			current = current->next;
		}
	}

	return num_results;
}

// Function: 		no_partition_thread
// Functionality:	thread function of no_partition_thread
// Input:		parameters from the main function
// Output:		void pointer
void * no_partition_thread(void* para)
{
	int rv;
	arg_t * args = (arg_t*) para;
	bucket_buffer_t* overflowbuf = initial_bucket_buffer();
	struct timeval time_1, time_2, time_3;
	// barrier until all threads start
	BARRIER_ARRIVE(args->barrier,rv);

	if(args->tid == 0)
	{
		// timing information
		gettimeofday(&time_1,NULL);
	}

	// build hash table
	build_hashtable(&args->relR, args->ht, &overflowbuf);

	// barrier until all threads finish the build
	BARRIER_ARRIVE(args->barrier,rv);

	if(args->tid == 0)
	{
		// timing information
		gettimeofday(&time_2,NULL);
	}

	args->num_results = probe_hashtable(&args->relS,args->ht);
/*	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
	probe_hashtable(&args->relS,args->ht);
*/
	// barrier until all threads finish the probe
	BARRIER_ARRIVE(args->barrier,rv);

	if(args->tid == 0)
	{
		// timing information
		gettimeofday(&time_3,NULL);

		printf("Build Phase for %ld microsec\n",(time_2.tv_sec-time_1.tv_sec)*1000000L+time_2.tv_usec-time_1.tv_usec);
		printf("Probe Phase for %ld microsec\n",(time_3.tv_sec-time_2.tv_sec)*1000000L+time_3.tv_usec-time_2.tv_usec);
	}

	free_bucket_buffer(overflowbuf);

	return 0;
}

int main(int argc, char* argv[])
{
	if(argc<2)
	{
		printf("Parameters error: use <execute file> <number of thread>\n");
		return 0;
	}
	else
	{
		struct timeval start_time, end_time;
		intnum_t numR,numS,numRthr,numSthr;
		intnum_t num_results = 0;
		int nthreads = atoi(argv[1]);
		arg_t args[nthreads];
		pthread_t tid[nthreads];
		pthread_attr_t attr;
		pthread_barrier_t barrier;
		cpu_set_t set;

		// start from CPU0/CPUX to make sure the data is generater there.
		cpu_set_t startset;
		CPU_ZERO(&startset);
		CPU_SET(0, &startset);
		if(sched_setaffinity(0, sizeof(startset), &startset) <0)
		{
			perror("sched_setaffinity\n");
		}

		numR = R_SIZE;
		numS = S_SIZE;
		numRthr = numR / nthreads;
		numSthr = numS / nthreads;
//		srand((unsigned)time(NULL));
		srand(12345);
		relation_t * relR = initial_relation(numR);
		relation_t * relS = initial_relation(numS);

		intnum_t num_buckets = relR->num_tuples*BUCKET_SIZE/BUCKET_SIZE;
//		intnum_t num_buckets = relR->num_tuples/BUCKET_SIZE;
		NEXT_POWER_TWO(num_buckets);
		hashtable_t * ht = initial_hashtable(num_buckets);

		// initial the barrier
		int rv = pthread_barrier_init(&barrier,NULL,nthreads);
		if(rv != 0)
		{
			printf("Could not create the barrier!\n");
			exit(EXIT_FAILURE);
		}

		// initial the attr
		pthread_attr_init(&attr);

		int num_cpus = sysconf( _SC_NPROCESSORS_ONLN );

		// Timing information
		gettimeofday(&start_time,NULL);

		// Build and probe the hash table in threads
		for(int i=0;i<nthreads;i++)
		{
		//	int cpu_idx = get_cpuid(i%MAX_NUM_THREAD);
			int cpu_idx = i%num_cpus;
			CPU_ZERO(&set);
			CPU_SET(cpu_idx,&set);
			pthread_attr_setaffinity_np(&attr,sizeof(cpu_set_t),&set);

			args[i].tid = i;
			args[i].ht = ht;
			args[i].barrier = &barrier;

			args[i].relR.num_tuples = (i==(nthreads-1))?numR:numRthr;
			args[i].relR.tuples = relR->tuples + numRthr * i;
			numR -= numRthr;

			args[i].relS.num_tuples = (i==(nthreads-1))?numS:numSthr;
			args[i].relS.tuples = relS->tuples + numSthr * i;
			numS -= numSthr;

			rv = pthread_create(&tid[i], &attr, no_partition_thread, (void*)&args[i]);
			if(rv)
			{
				printf("Thread create error with code: %d\n",rv);
				exit(-1);
			}
		}

		// join the threads
		for(int i=0;i<nthreads;i++)
		{
			pthread_join(tid[i],NULL);
			num_results += args[i].num_results;
		}

		// Timing information
		gettimeofday(&end_time,NULL);

		free_hashtable(ht);

		printf("%ld matched found!\n",num_results);
		printf("The program runs for %ld microsec\n",(end_time.tv_sec-start_time.tv_sec)*1000000L+end_time.tv_usec-start_time.tv_usec);
	}

	return 0;
}
