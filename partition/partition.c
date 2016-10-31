#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#include "lock.h"

//------------------ For Compiler only -------------------//
#ifdef __cplusplus
#define restrict		// define restrict for C++
#endif

//------------------ Constant Define----------------------//
#define RELATION_SIZE (1 << 24) 
#define RAMDOM_DATA		//enable this to use random data
#define CACHE_LINE_SIZE 64
#define ALIGNED_SIZE CACHE_LINE_SIZE
#define PREFETCH_DISTANCE 10
#define RELATION_PADDING 0	// need to set this
#define PADDING_TUPLES 0		// need to set this
#define SMALL_PADDING_TUPLES 0		// need to set this
#define NUM_RADIX_BITS 10		// need to set this
#define NUM_PASSES 2
#define FANOUT_PASS1 (1 << (NUM_RADIX_BITS/NUM_PASSES))
#define FANOUT_PASS2 (1 << (NUM_RADIX_BITS-(NUM_RADIX_BITS/NUM_PASSES)))
#define L1_CACHE_SIZE 32768	// need to set this
#define L1_CACHE_TUPLES (L1_CACHE_SIZE/sizeof(tuple_t))
#define THRESHOLD1(NTHR) (NTHR*L1_CACHE_TUPLES)
#define THRESHOLD2(NTHR) (NTHR*NTHR*L1_CACHE_TUPLES)

//------------------ Macro Define ------------------------//
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

// Define the hash function
// Use the num_buckets as a seed
// and the remainder as the hash value
#ifndef HASH
#define HASH(W,SEED,SHIFT)			\
	((W) & (SEED)) >> SHIFT
#endif

// Define the partition hash function
#ifndef PARTITION_HASH
#define PARTITION_HASH(K,R)		\
	(K) & ((1<<(R))-1)
#endif

// Define the barrier waiting
#ifndef BARRIER_ARRIVE
#define BARRIER_ARRIVE(B,RV)                            \
    RV = pthread_barrier_wait(B);                       \
    if(RV !=0 && RV != PTHREAD_BARRIER_SERIAL_THREAD){  \
        printf("Couldn't wait on barrier\n");           \
        exit(EXIT_FAILURE);                             \
    }
#endif

// Define checking malloc successful
#ifndef MALLOC_CHECK
#define MALLOC_CHECK(M)                                                 \
    if(!M){                                                             \
        printf("[ERROR] MALLOC_CHECK: %s : %d\n", __FILE__, __LINE__);  \
        perror(": malloc() failed!\n");                                 \
        exit(EXIT_FAILURE);                                             \
    }
#endif

#ifndef MAX
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
#endif

#ifndef THRESHOLD1
#define THRESHOLD1(NTHR) (NTHR*L1_CACHE_TUPLES)
#endif
//------------------ Type Define----------------------//
typedef int64_t intkey_t;
typedef int64_t intvalue_t;
typedef int64_t intnum_t;
typedef struct tuple_t tuple_t;
typedef struct relation_t relation_t;
typedef struct task_t task_t;
typedef struct task_list_t task_list_t;
typedef struct task_queue_t task_queue_t;
typedef struct arg_t  arg_t;
typedef struct part_t part_t;

typedef intnum_t (*JoinFunction)(const relation_t* const, const relation_t* const, intnum_t);

//---------------------- Structure Define ------------------------//
struct tuple_t
{
	intkey_t key;
	intvalue_t value;
};

struct relation_t
{
	tuple_t * tuples;
	intnum_t num_tuples;
};

struct task_t {
    relation_t relR;
    relation_t tmpR;
    relation_t relS;
    relation_t tmpS;
    task_t *   next;
};

struct task_list_t {
    task_t *      tasks;
    task_list_t * next;
    int           curr;
};

struct task_queue_t {
    pthread_mutex_t lock;
    pthread_mutex_t alloc_lock;
    task_t *        head;
    task_list_t *   free_list;
    int32_t         count;
    int32_t         alloc_size;
};

// arguments of each thread
struct arg_t {
    int32_t ** histR;
    tuple_t *  relR;
    tuple_t *  tmpR;
    int32_t ** histS;
    tuple_t *  relS;
    tuple_t *  tmpS;

    int32_t numR;
    int32_t numS;
    int32_t totalR;
    int32_t totalS;

    task_queue_t *      join_queue;
    task_queue_t *      part_queue;

    pthread_barrier_t * barrier;
    JoinFunction        join_function;
    int64_t result;
    int32_t my_tid;
    int     nthreads;

    /* stats about the thread */
    int32_t        parts_processed;
} __attribute__((aligned(CACHE_LINE_SIZE)));

// arguments of each partition
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// need to change type from int32 to int64
struct part_t {
    tuple_t *  rel;
    tuple_t *  tmp;
    int32_t ** hist;
    int32_t *  output;
    arg_t   *  thrargs;
    uint32_t   num_tuples;
    uint32_t   total_tuples;
    int32_t    R;
    uint32_t   D;
    int        relidx;  /* 0: R, 1: S */
    uint32_t   padding;
} __attribute__((aligned(CACHE_LINE_SIZE)));
//------------------------ Functions Define -----------------------//

// allocate memory function
void * alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, ALIGNED_SIZE, size);

    if (rv) {
        perror("[ERROR] alloc_aligned() failed: out of memory");
        return 0;
    }

    return ret;
};

relation_t * initial_relation(intnum_t num_tuples)
{
	relation_t * rel = (relation_t*)calloc(1,sizeof(relation_t));
	if(!rel)
	{
		perror("allocate relation error\n");
		exit(1);
	}

	rel->num_tuples = num_tuples;

	rel->tuples = (tuple_t*) alloc_aligned(rel->num_tuples * sizeof(tuple_t));
	if (!rel->tuples)
	 {
		perror("out of memory");
		exit(1);
	}

	srand((unsigned)time(NULL));
	for (intnum_t i=0;i<num_tuples;i++)
	{
#ifdef RAMDOM_DATA
		rel->tuples[i].key = i + rand();
#else
		rel->tuples[i].key = i;
#endif
		rel->tuples[i].value = num_tuples-i;
	}

	printf("Allocate relation done!\tSize:%ld\n",rel->num_tuples);

	return rel;
}


//--------------------- Task related function -------------------------//
task_queue_t* task_queue_init(int alloc_size)
{
    task_queue_t * ret = (task_queue_t*) malloc(sizeof(task_queue_t));
    ret->free_list = (task_list_t*) malloc(sizeof(task_list_t));
    ret->free_list->tasks = (task_t*) malloc(alloc_size * sizeof(task_t));
    ret->free_list->curr = 0;
    ret->free_list->next = NULL;
    ret->count      = 0;
    ret->alloc_size = alloc_size;
    ret->head       = NULL;
    pthread_mutex_init(&ret->lock, NULL);
    pthread_mutex_init(&ret->alloc_lock, NULL);

    return ret;
}

void task_queue_free(task_queue_t * tq)
{
    task_list_t * tmp = tq->free_list;
    while(tmp) {
        free(tmp->tasks);
        task_list_t * tmp2 = tmp->next;
        free(tmp);
        tmp = tmp2;
    }
    free(tq);
}

inline __attribute__((always_inline)) task_t* task_queue_get_slot(task_queue_t * tq)
{
  task_list_t * l = tq->free_list;
  task_t * ret;
  if(l->curr < tq->alloc_size)
	{
    ret = &(l->tasks[l->curr]);
    l->curr++;
  }
  else
	{
    task_list_t * nl = (task_list_t*) malloc(sizeof(task_list_t));
    nl->tasks = (task_t*) malloc(tq->alloc_size * sizeof(task_t));
    nl->curr = 1;
    nl->next = tq->free_list;
    tq->free_list = nl;
    ret = &(nl->tasks[0]);
  }

  return ret;
}

inline __attribute__((always_inline)) void task_queue_add(task_queue_t * tq, task_t * t)
{
  t->next  = tq->head;
  tq->head = t;
  tq->count ++;
}

inline __attribute__((always_inline)) task_t* task_queue_get_atomic(task_queue_t * tq) 
{
    pthread_mutex_lock(&tq->lock);
    task_t * ret = 0;
    if(tq->count > 0){
        ret      = tq->head;
        tq->head = ret->next;
        tq->count --;
    }
    pthread_mutex_unlock(&tq->lock);

    return ret;
}

inline __attribute__((always_inline)) task_t* task_queue_get_slot_atomic(task_queue_t * tq)
{
    pthread_mutex_lock(&tq->alloc_lock);
    task_t * ret = task_queue_get_slot(tq);
    pthread_mutex_unlock(&tq->alloc_lock);

    return ret;
}

inline __attribute__((always_inline)) void task_queue_add_atomic(task_queue_t * tq, task_t * t) 
{
    pthread_mutex_lock(&tq->lock);
    t->next  = tq->head;
    tq->head = t;
    tq->count ++;
    pthread_mutex_unlock(&tq->lock);

}

//---------------------------- End of task related functions ----------------------//

intnum_t bucket_chaining_join(const relation_t* const R, const relation_t* const S, intnum_t num_radix_bits)
{
	const intnum_t num_tuples_R = R->num_tuples;
	const intnum_t num_tuples_S = S->num_tuples;
	intnum_t results = 0;
	intnum_t N = num_tuples_R;
	NEXT_POWER_TWO(N);

	const intnum_t mask = (N-1) << num_radix_bits;

	int* next = (int*) malloc(sizeof(int)*num_tuples_R);
	int* bucket = (int*) calloc(N,sizeof(int));

	// build hash table
	for(intnum_t i=0;i<num_tuples_R;i++)
	{
		intnum_t index = HASH(R->tuples[i].key,mask,num_radix_bits);
		next[i] = bucket[index];
		bucket[index] = i+1;
	}
	// probe the hash table
	for(intnum_t i=0;i<num_tuples_S;i++)
	{
		intnum_t index = HASH(S->tuples[i].key,mask,num_radix_bits);
		for(intnum_t hit=bucket[index]; hit>0; hit=next[hit-1])
		{
			if(S->tuples[i].key == R->tuples[hit-1].key)
			{
				results ++;
			}
		}
	}

	return results;
}

void parallel_radix_partition(part_t * const part)
{
	const tuple_t * restrict rel    = part->rel;
	int32_t **               hist   = part->hist;
	int32_t *       restrict output = part->output;

	const uint32_t my_tid     = part->thrargs->my_tid;
	const uint32_t nthreads   = part->thrargs->nthreads;
	const uint32_t num_tuples = part->num_tuples;

	const int32_t  R       = part->R;
	const int32_t  D       = part->D;
	const uint32_t fanOut  = 1 << D;
	const uint32_t MASK    = (fanOut - 1) << R;
	const uint32_t padding = part->padding;

	int32_t sum = 0;
	uint32_t i, j;
	int rv;

	int32_t dst[fanOut+1];

	/* compute local histogram for the assigned region of rel */
	/* compute histogram */
	int32_t * my_hist = hist[my_tid];

	for(i = 0; i < num_tuples; i++)
	{
		uint32_t idx = HASH(rel[i].key, MASK, R);
		my_hist[idx] ++;
	}

	/* compute local prefix sum on hist */
	for(i = 0; i < fanOut; i++)
	{
		sum += my_hist[i];
		my_hist[i] = sum;
	}

	/* wait at a barrier until each thread complete histograms */
	BARRIER_ARRIVE(part->thrargs->barrier, rv);
	
	/* determine the start and end of each cluster */
	for(i = 0; i < my_tid; i++)
	{
		for(j = 0; j < fanOut; j++)
			output[j] += hist[i][j];
	}

	for(i = my_tid; i < nthreads; i++)
	{
		for(j = 1; j < fanOut; j++)
			output[j] += hist[i][j-1];
	}

	for(i = 0; i < fanOut; i++ )
	{
		output[i] += i * padding; //PADDING_TUPLES;
		dst[i] = output[i];
	}
	output[fanOut] = part->total_tuples + fanOut * padding; //PADDING_TUPLES;

	tuple_t * restrict tmp = part->tmp;

	/* Copy tuples to their corresponding clusters */
	for(i = 0; i < num_tuples; i++ )
	{
		uint32_t idx = HASH(rel[i].key, MASK, R);
		tmp[dst[idx]] = rel[i];
		++dst[idx];
	}
}

void parallel_radix_partition_optimized(part_t * const part)
{
	// TODO: optimized
}

void 
radix_cluster(relation_t * restrict outRel, 
              relation_t * restrict inRel,
              int32_t * restrict hist, 
              int R, 
              int D)
{
    uint32_t i;
    uint32_t M = ((1 << D) - 1) << R;
    uint32_t offset;
    uint32_t fanOut = 1 << D;

    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function 
       just in case D differs from call to call. */
    uint32_t dst[fanOut];

    /* count tuples per cluster */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t idx = HASH(inRel->tuples[i].key, M, R);
        hist[idx]++;
    }
    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        /* dst[i]      = outRel->tuples + offset; */
        /* determine the beginning of each partitioning by adding some
           padding to avoid L1 conflict misses during scatter. */
        dst[i] = offset + i * SMALL_PADDING_TUPLES;
        offset += hist[i];
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t idx   = HASH(inRel->tuples[i].key, M, R);
        outRel->tuples[ dst[idx] ] = inRel->tuples[i];
        ++dst[idx];
    }
}

void serial_radix_partition(task_t * const task, task_queue_t * join_queue, const int R, const int D)
{
	int i;
	uint32_t offsetR = 0, offsetS = 0;
	const int fanOut = 1 << D;  /*(NUM_RADIX_BITS / NUM_PASSES);*/
	int32_t * outputR, * outputS;

	outputR = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
	outputS = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
	/* TODO: measure the effect of memset() */
	/* memset(outputR, 0, fanOut * sizeof(int32_t)); */
	radix_cluster(&task->tmpR, &task->relR, outputR, R, D);

	/* memset(outputS, 0, fanOut * sizeof(int32_t)); */
	radix_cluster(&task->tmpS, &task->relS, outputS, R, D);

	/* task_t t; */
	for(i = 0; i < fanOut; i++)
	{
		if(outputR[i] > 0 && outputS[i] > 0)
		{
			task_t * t = task_queue_get_slot_atomic(join_queue);
			t->relR.num_tuples = outputR[i];
			t->relR.tuples = task->tmpR.tuples + offsetR 
				     + i * SMALL_PADDING_TUPLES;
			t->tmpR.tuples = task->relR.tuples + offsetR 
				     + i * SMALL_PADDING_TUPLES;
			offsetR += outputR[i];

			t->relS.num_tuples = outputS[i];
			t->relS.tuples = task->tmpS.tuples + offsetS 
				     + i * SMALL_PADDING_TUPLES;
			t->tmpS.tuples = task->relS.tuples + offsetS 
				     + i * SMALL_PADDING_TUPLES;
			offsetS += outputS[i];

			/* task_queue_copy_atomic(join_queue, &t); */
			task_queue_add_atomic(join_queue, t);
		} 
		else
		{
			offsetR += outputR[i];
			offsetS += outputS[i];
		}
	}
		free(outputR);
		free(outputS);
}

// This is the thread function
void* prj_thread(void* param)
{
	arg_t * args   = (arg_t*) param;
	int32_t my_tid = args->my_tid;

	const int fanOut = 1 << (NUM_RADIX_BITS / NUM_PASSES);
	const int R = (NUM_RADIX_BITS / NUM_PASSES);
	const int D = (NUM_RADIX_BITS - (NUM_RADIX_BITS / NUM_PASSES));
	const int thresh1 = MAX((1<<D), (1<<R)) * THRESHOLD1(args->nthreads);

	uint64_t results = 0;
	int i;
	int rv;

	part_t part;
	task_t * task;
	task_queue_t * part_queue;
	task_queue_t * join_queue;

	int32_t * outputR = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
	int32_t * outputS = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
	MALLOC_CHECK((outputR && outputS));

	part_queue = args->part_queue;
	join_queue = args->join_queue;

	args->histR[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));
	args->histS[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));

	/* in the first pass, partitioning is done together by all threads */

	args->parts_processed = 0;

	// barrier: wait for all threads start
	BARRIER_ARRIVE(args->barrier, rv);

	//-------------------- 1st pass partitioning ---------------------------//
	part.R       = 0;
	part.D       = NUM_RADIX_BITS / NUM_PASSES;
	part.thrargs = args;
	part.padding = PADDING_TUPLES;

	// 1. partitioning for relation R
	part.rel          = args->relR;
	part.tmp          = args->tmpR;
	part.hist         = args->histR;
	part.output       = outputR;
	part.num_tuples   = args->numR;
	part.total_tuples = args->totalR;
	part.relidx       = 0;

#ifdef USE_SWWC_OPTIMIZED_PART
	parallel_radix_partition_optimized(&part);
#else
	parallel_radix_partition(&part);
#endif

	// 2. partitioning for relation S
	part.rel          = args->relS;
	part.tmp          = args->tmpS;
	part.hist         = args->histS;
	part.output       = outputS;
	part.num_tuples   = args->numS;
	part.total_tuples = args->totalS;
	part.relidx       = 1;

#ifdef USE_SWWC_OPTIMIZED_PART
    parallel_radix_partition_optimized(&part);
#else
    parallel_radix_partition(&part);
#endif


  /* wait at a barrier until each thread copies out */
  BARRIER_ARRIVE(args->barrier, rv);

  //------------------- end of 1st partitioning phase ---------------------//

	// 3. first thread creates partitioning tasks for 2nd pass
	if(my_tid == 0)
	{
		for(i = 0; i < fanOut; i++)
		{
			int32_t ntupR = outputR[i+1] - outputR[i] - PADDING_TUPLES;
			int32_t ntupS = outputS[i+1] - outputS[i] - PADDING_TUPLES;
			if(ntupR > 0 && ntupS > 0)
			{
				task_t * t = task_queue_get_slot(part_queue);

				t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
				t->relR.tuples = args->tmpR + outputR[i];
				t->tmpR.tuples = args->relR + outputR[i];

				t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
				t->relS.tuples = args->tmpS + outputS[i];
				t->tmpS.tuples = args->relS + outputS[i];

				task_queue_add(part_queue, t);
			}
		}
	}
	
	// barrier wait for adding all partitioning tasks */
	BARRIER_ARRIVE(args->barrier, rv);
	
	//----------------- 2nd pass  partitioning --------------------//
	//4. 2nd partitioning and add join task queue

#if NUM_PASSES==1
	// If the partitioning is single pass we directly add tasks from pass-1
	task_queue_t * swap = join_queue;
	join_queue = part_queue;
	// part_queue is used as a temporary queue for handling skewed parts
	part_queue = swap;
    
#elif NUM_PASSES==2
	while((task = task_queue_get_atomic(part_queue)))
	{
		serial_radix_partition(task, join_queue, R, D);
	}

#else
#warning Only 2-pass partitioning is implemented, set NUM_PASSES to 2!
#endif

	free(outputR);
	free(outputS);
	
	// wait at a barrier until all threads add all join tasks
	BARRIER_ARRIVE(args->barrier, rv);
	
	while((task = task_queue_get_atomic(join_queue)))
	{
        	/* do the actual join. join method differs for different algorithms,
        	i.e. bucket chaining, histogram-based, histogram-based with simd &
        	prefetching  */
		results += args->join_function(&task->relR, &task->relS, NUM_RADIX_BITS);
                
        	args->parts_processed ++;
	}

	args->result = results; // get the result
}

intnum_t join_init_run(relation_t* relR, relation_t* relS, JoinFunction jf, int nthreads)
{
	intnum_t results = 0;

	// TODO: initial for the threads,then call the threadfunction
	int i, rv;
	pthread_t tid[nthreads];
	pthread_attr_t attr;
	pthread_barrier_t barrier;
	arg_t args[nthreads];

	int32_t ** histR, ** histS;
	tuple_t * tmpRelR, * tmpRelS;
	int32_t numperthr[2];

	task_queue_t * part_queue, * join_queue;

	part_queue = task_queue_init(FANOUT_PASS1);
	join_queue = task_queue_init((1<<NUM_RADIX_BITS));

	// allocate temporary space for partitioning
	tmpRelR = (tuple_t*) alloc_aligned(relR->num_tuples * sizeof(tuple_t) + RELATION_PADDING);
	tmpRelS = (tuple_t*) alloc_aligned(relS->num_tuples * sizeof(tuple_t) + RELATION_PADDING);

	MALLOC_CHECK((tmpRelR && tmpRelS));

	/* allocate histograms arrays, actual allocation is local to threads */
	histR = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
	histS = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
	MALLOC_CHECK((histR && histS));

	rv = pthread_barrier_init(&barrier, NULL, nthreads);
	if(rv != 0)
	{
			printf("[ERROR] Couldn't create the barrier\n");
			exit(EXIT_FAILURE);
	}

	pthread_attr_init(&attr);

	// assign chunks of relR & relS for each thread
	numperthr[0] = relR->num_tuples / nthreads;
	numperthr[1] = relS->num_tuples / nthreads;

	for(i = 0; i < nthreads; i++)
	{
			args[i].relR = relR->tuples + i * numperthr[0];
			args[i].tmpR = tmpRelR;
			args[i].histR = histR;

			args[i].relS = relS->tuples + i * numperthr[1];
			args[i].tmpS = tmpRelS;
			args[i].histS = histS;

			args[i].numR = (i == (nthreads-1)) ?
					(relR->num_tuples - i * numperthr[0]) : numperthr[0];
			args[i].numS = (i == (nthreads-1)) ?
					(relS->num_tuples - i * numperthr[1]) : numperthr[1];
			args[i].totalR = relR->num_tuples;
			args[i].totalS = relS->num_tuples;

			args[i].my_tid = i;
			args[i].part_queue = part_queue;
			args[i].join_queue = join_queue;

			args[i].barrier = &barrier;
			args[i].join_function = jf;
			args[i].nthreads = nthreads;

			rv = pthread_create(&tid[i], &attr, prj_thread, (void*)&args[i]);
			if (rv){
					printf("[ERROR] return code from pthread_create() is %d\n", rv);
					exit(-1);
			}
	}

	// Join all the threads and count the final result
	for(i = 0; i < nthreads; i++){
			pthread_join(tid[i], NULL);
			results += args[i].result;
	}

	// free the space
	for(i = 0; i < nthreads; i++) {
			free(histR[i]);
			free(histS[i]);
	}
	free(histR);
	free(histS);
	task_queue_free(part_queue);
	task_queue_free(join_queue);

	free(tmpRelR);
	free(tmpRelS);

	return results;
}

intnum_t partition_hash_join(relation_t* relR, relation_t* relS, int nthreads)
{
	intnum_t results = 0;

	results = join_init_run(relR,relS,bucket_chaining_join,nthreads);

	return results;
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
		const intnum_t num_tuples = RELATION_SIZE;
		const int nthreads = atoi(argv[1]);
		struct timeval start_time, end_time;

		relation_t * relR = initial_relation(num_tuples);
		relation_t * relS = initial_relation(num_tuples*16);

		// Timing information
		gettimeofday(&start_time,NULL);

		// Build and probe the hash table.
		intnum_t num_results = partition_hash_join(relR,relS,nthreads);

		// Timing information
		gettimeofday(&end_time,NULL);

		printf("%ld matched found!\n",num_results);
		printf("The program runs for %ld microsec\n",(end_time.tv_sec-start_time.tv_sec)*1000000L+end_time.tv_usec-start_time.tv_usec);

		return 0;
	}
}
