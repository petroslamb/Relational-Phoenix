
#include "operators.h"
#include "main.h"

/**
 * PRINTS the data returned by the load(fd) function
 */
void printLoads(filedata_t *fd)
{
	int num_records, i=0;
	num_records = (int)(fd->flen)/(int)(sizeof(record));  
	for(i=0; i<num_records;i++){

		record *rec = &(fd->f_data)[i*(int)(sizeof(record))];
		printf("record length : %d \n",rec->length);
		printf("record recnum : %d \n",rec->recnum);
		printf("record key : %d \n",rec->key);
		printf("record value : %s \n",rec->value);
	}
}

/** Prints results coming from the operators
 *  Use of records as data structures
 */
void printAggr(final_data_t op_results){


	//Required stuff

	int i = 0;

	for (i = 0; i <  op_results.length; i++){

		keyval_t * curr = &((keyval_t *)op_results.data)[i];
//		dprintf("MRkey: %10s ", curr->key);
//		dprintf("MRvalue: %d ", *(intptr_t *)curr->val);
      dprintf("%10s - %" PRIdPTR "\n", (char *)curr->key, (intptr_t)curr->val);
	}

}

/** Prints results coming from the operators
 *  Use of records as data structures
 */
void print_sel(final_data_t op_results){


	//Required stuff

	int i = 0;
	record *value;


	/*
	   WORKS key int, record as value
	   */

	for (i = 0; i <  op_results.length; i++){

		keyval_t * curr = &((keyval_t *)op_results.data)[i];
		value = curr->val;
		dprintf("value: %10s ", value->value);
		dprintf("MR key in keyval pair: %d ", *(int *)curr->key);
		dprintf("recnum: %d ", value->recnum);
		dprintf("key: %d ", value->key);
		printf("record length : %d \n",value->length);
	}

}



/** Prints results coming from the operators
 *  Use of records as data structures
 */
void print_prj(final_data_t op_results){

	typedef struct {
		char table;
		int recnum;
		int length;
		char *value;
		char *data;

	}new_rec;

	//Required stuff

	int i = 0;
	new_rec *value;


	/*
	   WORKS key int, record as value
	   */

	for (i = 0; i <  op_results.length; i++){

		keyval_t * curr = &((keyval_t *)op_results.data)[i];
		value = curr->val;
		dprintf("value: %10s ", value->value);
		dprintf("MR key in keyval pair: %d ", *(int *)curr->key);
		dprintf("recnum: %d ", value->recnum);
		printf("record length : %d \n",value->length);
	}


}

void print_srt(final_data_t op_results){

		//Required stuff

	int i = 0;
	record_new *value;
	char *key;

	/*
	   WORKS key int, record as value
	   */

	for (i = 0; i <  op_results.length; i++){

		keyval_t * curr = &((keyval_t *)op_results.data)[i];
		value = curr->val;
		key = curr->key;
		dprintf("value: %10s ", value->value);
		dprintf("MR key in keyval pair: %10s ", key);
		dprintf("recnum: %d ", value->recnum);
		printf("record length : %d \n",value->length);
	}


}


void print_prt(final_data_t op_results){

		//Required stuff

	int i = 0;
	record_new *value;
	int *key;

	/*
	   WORKS key int, record as value
	   */

	for (i = 0; i <  op_results.length; i++){

		keyval_t * curr = &((keyval_t *)op_results.data)[i];
		value = curr->val;
		key = curr->key;
		dprintf("value: %10s ", value->value);
		dprintf("MR key in keyval pair: %d ", *key);
		dprintf("recnum: %d ", value->recnum);
		printf("record length : %d \n",value->length);
	}


}



	/*
WORKS: prints record as key, number as value. Requires appropriate mapper, reducer, comp
*/
	/*
	   for (i = 0; i < op_results.length; i++)
	   {
	   keyval_t * curr = &((keyval_t *)op_results.data)[i];
	   dprintf(" key : %" PRIdPTR "\n", (intptr_t)curr->val);
	   value = curr->key;
	   dprintf("string : %10s \n", value->value);
	   }

*/

	/*
	   WORKS both key and value are ints requires appropriate 
	   */
	/*
	   for (i = 0; i <  op_results.length; i++)
	   {
	   keyval_t * curr = &((keyval_t *)op_results.data)[i];
	   dprintf("value: %" PRIdPTR "\n", (intptr_t)curr->val);
	   dprintf("key: %d \n", *(int *)(curr->key));
	   }

*/


