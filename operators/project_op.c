
#include "operators.h"

record_t *(*prj_fptr)(void *ptr);
int num_reducers;

/**
 * Projection algorithm
 */
void projection(record_t *record){

	record_t *temp = prj_fptr(record);
	emit_intermediate( &(temp->recnum), (void *)temp, (int)(sizeof(int)) );

}


/** map_prj()
 * Go through the allocated portion of the file and apply the operator
 */
void mapper_prj(map_args_t *args) 
{
	int k, i=0;
	//int length; 
	//char *data;

	assert(args);
	keyval_t * keyvalue = (keyval_t *)args->data;
	assert(keyvalue);
	record_t * record = (record_t *)keyvalue->val;
	assert(record);



	//	printf("map_args_t length: %d \n",(int)args->length);

	while(i < (args->length)){
		//printf("record_t length %d, record number: %d \n", record->length, record->recnum);
		//length = record->length;
		//data = (char *)record;

		projection(record);

		i++;//=sizeof(keyval_t);
		record = (record_t *)(keyvalue[i].val);
		//	assert(record);
	}


}




void project_op(final_data_t *in, record_t *(*prj)(void *rec),final_data_t *op_results, int op_num){

	struct timeval begin, end;
#ifdef TIMING
	unsigned int library_time = 0;
#endif

	//final_data_t *in = (final_data_t *)void_in;
	size_t keyval_size = sizeof(keyval_t);
	prj_fptr = prj;

	int env_threads = atoi(GETENV("MR_NUMTHREADS"));
	int proc_threads = get_nprocs();


	struct timeval begin_op, end_op;


	printf("\nprojection operator  \n");
	printf("results size %d \n",in->length);
	printf("results * unitsize %d \n",(in->length)*keyval_size);


	get_time (&begin_op);

	//Setup map reduce args
	map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
	map_reduce_args.task_data = in->data;
	map_reduce_args.map = mapper_prj;
	map_reduce_args.reduce = NULL;//reduce; // Identity Reduce
	map_reduce_args.splitter = NULL;//splitter2; 
	map_reduce_args.key_cmp = ( (op_num==1)? integercmp : nullcmp );
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
	fprintf (stderr, "Project time: %u\n\n", time_diff (&end_op, &begin_op));
#endif

}


