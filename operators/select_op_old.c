
#include "operators.h"

/*
   struct timeval begin, end;
#ifdef TIMING
unsigned int library_time = 0;
#endif*/


int (*fptr)(void *ptr);
//void (*alg_ptr)(map_args_t *args);



/** splitter()
 *  Memory map the file and divide file on a word border i.e. a space.
 */
int splitter_old(void *data_in, int req_units, map_args_t *out)
{
	filedata_t * data = (filedata_t *)data_in; 

	assert(data_in);
	assert(out);

	assert(data->flen >= 0);
	assert(data->f_data);
	assert(req_units);
	assert(data->fpos >= 0);

	// End of file reached, return FALSE for no more data
	if (data->fpos >= data->flen) return 0;

	// Set the start of the next data
	out->data = (void *)&data->f_data[data->fpos];

	// Determine the nominal length
	out->length = req_units * data->unit_size;


	if (data->fpos + out->length > data->flen)
		out->length = data->flen - data->fpos;
	// printf("chunk length: %d \n",out->length);
	data->fpos += (long)out->length;

	//     // TODO Set the length to end at a record end
	//     for (data->fpos += (long)out->length;
	//           data->fpos < data->flen;
	//           data->fpos++, out->length++);


	return 1;
}


/**
 * Selection algorithm
 */
void selection_old(record_t *record)
{
	if(fptr(record) == 1) emit_intermediate( &(record->recnum), (void *)record, (int)(sizeof(int)) );

}

/**
 *  extraction algorithm from initially mapped data
 */
void extraction_old(void (*algorithm)(record_t *record), map_args_t *args)
{
	int length, k, i=0;
	char *data;
	assert(args);
	length = args->length;
	assert(length);

	record_t * record = (record_t *)args->data;

	//printf("map_args_t length: %d \n",(int)args->length);

	k=0;
	while(i < (args->length)){

		//printf("record_t length %d, record number: %d \n", record->length, record->recnum);

		assert(record);
		algorithm(record);

		length = record->length;
		data = (char *)record;
		record = (record_t *)(&data[length]);
		i+=length;
		k++;
	}

}





/** map()
 * Go through the allocated portion of the file and apply the operator
 */
void map_old(map_args_t *args) 
{

	extraction_old(&selection_old, args);

}



void select_op_old(filedata_t *in, int (*cmp)(void *rec),final_data_t *op_results, int op_num){

	struct timeval begin, end;
#ifdef TIMING
	unsigned int library_time = 0;
#endif
	struct timeval begin_op, end_op;

	fptr = cmp;
	//alg_ptr = selection;
	//	printf("\n operator % \n",op_num);


	printf("\nOLD selection operator \n");
	printf("results size %d \n",in->flen);
	printf("results * unitsize %d \n",(in->flen) * (int)in->unit_size);


	get_time (&begin_op);
	//Setup map reduce args
	map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
	map_reduce_args.task_data = in;
	map_reduce_args.map = map_old;
	map_reduce_args.reduce = NULL;//reduce; // Identity Reduce
	map_reduce_args.splitter = splitter_old; 
	map_reduce_args.key_cmp = ( (op_num==1)? integercmp : nullcmp );
	map_reduce_args.unit_size = in->unit_size;
	map_reduce_args.partition = NULL; 
	map_reduce_args.result = op_results;
	map_reduce_args.data_size = in->flen;
	map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));
	map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));
	map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
	map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
	map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
	map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;
	map_reduce_args.use_one_queue_per_task = atoi(GETENV("MR_1QPERTASK")) ? true : false;


	get_time(&begin);
	CHECK_ERROR(map_reduce (&map_reduce_args) < 0);
	get_time(&end);

#ifdef TIMING
	library_time += time_diff (&end, &begin);
	fprintf (stderr, "library: %u\n", library_time);
#endif


	get_time (&end_op);

#ifdef TIMING
	fprintf (stderr, "Select_old time: %u\n\n", time_diff (&end_op, &begin_op));
#endif



}
