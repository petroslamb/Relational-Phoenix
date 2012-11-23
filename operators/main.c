
#include "operators.h"
#include "main.h"


/** cmp12()
 *  Comparison function to compare records with predicate
 */
int cmp12(void *rec)
{

	typedef struct {
		char table;
		int recnum;
		int length;
		char value[10];
		int key;
		char data[80];

	}rec_str;

	rec_str *r=(rec_str *)rec;

	//printf("cmp12 call on, %d \n",r->recnum);
	if (r->key > 12) {return 1;}
	else return 0;


}

/** prj5()
 *  Comparison function to compare records with predicate
 */

record_t *prj5(void *rec)
{

	typedef struct {
		char table;
		int recnum;
		int length;
		char value[10];
		int key;
		char data[80];

	}old_rec;


	typedef struct {
		char table;
		int recnum;
		int length;
		char *value;
		char *data;

	}new_rec;

	new_rec *new_r = malloc(sizeof(new_rec));
	old_rec *old_r=(old_rec *)rec;
	record_t *record;

	new_r->table = old_r->table;
	new_r->recnum = old_r->recnum;
	new_r->length =(int)sizeof(record_new);
	new_r->value = (old_r->value);
	new_r->data = (old_r->data);

	record = (record_t *)new_r;

	//	printf("prj call on value field, old:new->recnum:%d : %d \n",old_r->recnum, new_r->recnum);

	return record;
}



int hsh_prt (void * record) {

	typedef struct {
		char table;
		int recnum;
		int length;
		char value[10];
		int key;
		char data[80];

	}new_rec;

	new_rec *r = record;
	//void *key = r->value;
	int key_size = 10 * sizeof(char);
	int i, sum;

	char *ch = (char *)r->value;
	for (sum=0, i=0; i < key_size; i++) sum += ch[i];
	return sum % 4;

}

/** aggr()
 *  aggregation function, must return type of key 
 */
void *aggr (iterator_t *itr)
{
	void *val;
	intptr_t sum = 0;

	assert(itr);

	while (iter_next (itr, &val))
	{
		sum += (intptr_t)val;
	}

	return (void *)sum;
}



/** key_ptr()
 *  Returns the pointer to the key on which to sort the records
 */
void *key_ptr(void *rec){

	typedef struct {
		char table;
		int recnum;
		int length;
		char value[10];
		int key;
		char data[80];

	}new_rec;

	new_rec *r = (new_rec *)rec;
	//char *char_ptr = r->value;
	return (void *)r->value;
}

/** keysize
 * Returns the size of key for given record
 */
int keysize(void *rec){
	return 10*((int)sizeof(char));
}

/** stringcmp()
 *  Comparison function to compare 2 words
 */
int stringcmp(const void *s1, const void *s2)
{
	return strcmp((const char *)s1, (const char *) s2);
}

/** aggr_ptr()
 *  Returns pointer to aggregate value, for received record
 */
void *aggr_ptr(void *rec)
{
	return (void *)1;
}



int main(int argc, char *argv[]) 
{
	int i = 0;
	int *key;
	record *value;
	filedata_t *fd;
	final_data_t op_results;
	final_data_t op_results0;
	final_data_t op_results2;
	final_data_t op_results3;
	final_data_t op_results4;
	final_data_t op_results5;
	final_data_t op_results6;
	final_data_t op_results7;

	struct timeval starttime,endtime;
	char * fname, * op_num_str;
	int op_num;

	struct timeval begin_total, end_total;


	// Make sure a filename is specified
	if (argv[1] == NULL)
	{
		printf("USAGE: %s <filename> [ # of operator to use (1:sel 2:prj 3:srt 4:prt 5:agg) ]\n", argv[0]);
		exit(1);
	}

	fname = argv[1];
	op_num_str = argv[2];

	// Get the number of operator to run
	CHECK_ERROR((op_num = (op_num_str == NULL) ? 10 : atoi(op_num_str)) < 0);



	fd=load_op (fname, sizeof(record));	
		printLoads(fd);
	
	if ((op_num  != 6) && (op_num != 0)){
		prepare_op(fd, &op_results, 0);
		dprintf("\nop_results length  %d:\n", op_results.length);
	}
	get_time (&begin_total);

	switch(op_num){
		case 0:
			prepare_op(fd, &op_results0, 1);

			break;
		case 1:
			select_op(&op_results, cmp12, &op_results2, 1);
				print_sel(op_results2);
			break;
		case 2:
			project_op(&op_results, prj5, &op_results3, 1);
				print_prj(op_results3);
			break;
		case 3:
			sort_op(&op_results, stringcmp, key_ptr, keysize, &op_results4, 1);
				print_srt(op_results4);
			break;
		case 4:
			partition_op(&op_results, hsh_prt, &op_results5, 1);
				print_prt(op_results5);

			//	partition_2op(&op_results, prt, &op_results5, 0);
			break;
		case 5:
			aggregate_op(&op_results, aggr, stringcmp, key_ptr, keysize, aggr_ptr, &op_results6, 1);
				printAggr(op_results6);
			break;
		case 6:
			select_op_old(fd,cmp12, &op_results7, 1);
				//print_sel(op_results7);
			break;
		default:
			printf("No operator selected\n");
			break;
	}

	get_time (&end_total);
	unload_op(fd);


#ifdef TIMING
	fprintf (stderr, "\n\nOverall time: %u\n\n\n", time_diff (&end_total, &begin_total));
#endif
	return 1;
}

