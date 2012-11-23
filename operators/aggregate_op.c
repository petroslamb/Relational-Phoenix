
#include "operators.h"


static int (*srt_fptr)(const void *k1, const void *k2);
void *(*aggr_ptr_fptr)(void *ptr);
void *(*aggr_fptr)(iterator_t *itr); 
void *(*key_fptr)(void *ptr);
int (*ksize_fptr)(void *ptr);

int num_reducers;

/**
 *  Key_emit 
 */
void key_emit(record_t *record){

	//char *temp = (char *)key_fptr(record);
	emit_intermediate( key_fptr(record), aggr_ptr_fptr(record), ksize_fptr(record) );

}


/** map_aggr()
 * Go through the allocated portion of the file and apply the operator
 */
void mapper_aggr(map_args_t *args) 
{

	int length, k, i=0;
	//char *data;

	assert(args);
	keyval_t * keyvalue = (keyval_t *)args->data;
	assert(keyvalue);
	record_t * record = (record_t *)keyvalue->val;
	assert(record);



	//	printf("map_args_t length: %d \n",(int)args->length);

	while(i < (args->length)){
		//printf("record_t length %d, record number: %d \n", record->length, record->recnum);
		length = record->length;
		//data = (char *)record;

		key_emit(record);

		i++;//=sizeof(keyval_t);
		record = (record_t *)(keyvalue[i].val);
		//	assert(record);
	}


}

/** aggr_reduce()
 * Add up the partial sums for each word
 */
void aggr_reduce(void *key_in, iterator_t *itr)
{
    	    	emit (key_in, (void *)aggr_fptr(itr));
	//	printf("thread id: %d \n", thread_index);
}

/** aggr_combiner
 *
 */

void *aggr_combiner (iterator_t *itr)
{
	return aggr(itr);
}


void aggregate_op(final_data_t *in, void *(*aggr)(iterator_t *itr), int (*srt)(const void *k1, const void *k2), void *(*key_ptr)(void *rec), int (*keysize)(void *rec), void *(*aggr_ptr)(void *), final_data_t *op_results, int op_num){

	struct timeval begin, end;
#ifdef TIMING
	unsigned int library_time = 0;
#endif

	//final_data_t *in = (final_data_t *)void_in;
	size_t keyval_size = sizeof(keyval_t);

	srt_fptr = srt;
	aggr_fptr = aggr;
	aggr_ptr_fptr = aggr_ptr;
	key_fptr = key_ptr;
	ksize_fptr = keysize;
	int env_threads = atoi(GETENV("MR_NUMTHREADS"));
	int proc_threads = get_nprocs();


	struct timeval begin_op, end_op;
	printf("\naggregation operator \n");
	printf("results size %d \n",in->length);
	printf("results * unitsize %d \n",(in->length)*keyval_size);


	get_time (&begin_op);
	//Setup map reduce args
	map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
	map_reduce_args.task_data = in->data;
	map_reduce_args.map = mapper_aggr;
	map_reduce_args.combiner = aggr_combiner;
	map_reduce_args.reduce = aggr_reduce; //NULL; // Identity Reduce
	map_reduce_args.splitter = NULL;// Array Splitter 
	map_reduce_args.key_cmp = srt_fptr;
	map_reduce_args.unit_size = keyval_size;
	map_reduce_args.partition = NULL; 
	map_reduce_args.result = op_results;
	map_reduce_args.data_size = (in->length) * keyval_size;
	map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));
	map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));
	map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
	map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
	map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
	map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;
	map_reduce_args.use_one_queue_per_task = atoi(GETENV("MR_1QPERTASK")) ? true : false;

	num_reducers = ((env_threads > 0)?env_threads:proc_threads);

	printf("number of reducers: %d \n",num_reducers);	

	get_time(&begin);
	CHECK_ERROR(map_reduce (&map_reduce_args) < 0);
	get_time(&end);

#ifdef TIMING
	library_time += time_diff (&end, &begin);
	fprintf (stderr, "library: %u\n", library_time);
#endif

	get_time (&end_op);

#ifdef TIMING
	fprintf (stderr, "Aggregation time: %u\n\n", time_diff (&end_op, &begin_op));
#endif

}


