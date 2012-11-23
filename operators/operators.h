
#ifndef _OPERATORS_H_
#define _OPERATORS_H_

#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <ctype.h>
#include <inttypes.h>
#include <time.h>


#include "map_reduce.h"
#include "stddefines.h"

typedef struct {
	char table;
	int recnum;
	int length;
	char *rec_ptr;

}record_t;



typedef struct {
	int fpos;
	int fhandle;
	off_t flen;
	char *f_data;
	int unit_size;

} filedata_t;



int nullcmp(const void *, const void *);

int integercmp(const void *, const void *);

int stringcmp(const void *, const void *);

filedata_t *load_op(char *,size_t);

void unload_op(filedata_t *);

record_t *extract(filedata_t *);


void prepare_op(filedata_t *,final_data_t *, int );

void select_op(final_data_t *, int (*sel)(void *),final_data_t *, int );

void project_op(final_data_t *, record_t *(*fptr)(void *), final_data_t *, int);	

void sort_op(final_data_t *, int (*fptr)(const void *, const void *),  void *(*fptr2)(void *), int (*fptr3)(void *), final_data_t *, int);

void partition_op(final_data_t *, int (*fptr)(void *), final_data_t *, int);

//void partition_2op(final_data_t *, int (*fptr)(void *), final_data_t *, int);

void aggregate_op(final_data_t *, void *(*aggr)(iterator_t *), int (*fptr)(const void *, const void *),  void *(*key_ptr)(void *), int (*keysize)(void *), void *(*aggr_ptr)(void *), final_data_t *, int);

void select_op_old(filedata_t *, int (*cmp)(void *),final_data_t *, int );

//void error_op(final_data_t *, int (*fptr)(void *, int), void *(*fptr2)(void *), int (*fptr3)(void *), final_data_t *, int);

#endif // _OPERATORS_H_
