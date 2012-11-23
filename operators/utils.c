

#include "operators.h"


/** nullcmp()
 *  Comparison function to return zero
 */
int nullcmp(const void *v1, const void *v2)
 {
     return 0;
 }



/** integercmp()
 *  Comparison function to compare 2 ints
 */
int integercmp(const void *v1, const void *v2)
 {
     return (*(int *)v1 - *(int *)v2);
 }

 
 

/** Opens file, memory maps and gets data. 
 *	Returns void pointer, pointing to structure with file data
 */
filedata_t *load_op(char *fname, size_t record_length){

	//int i;
    int fd;
    char * fdata;
   // int disp_num;
    struct stat finfo;
  //  char * disp_num_str;
    
	
	//Open file, put in memory, get file info. 
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    CHECK_ERROR(fstat(fd, &finfo) < 0);
	
#ifndef NO_MMAP
    
    CHECK_ERROR((fdata = mmap(0, finfo.st_size + 1, 
      PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
// #else
//     int ret;
//     fdata = (char *)malloc (finfo.st_size);
//     CHECK_ERROR (fdata == NULL);
//     ret = read (fd, fdata, finfo.st_size);
//     CHECK_ERROR (ret != finfo.st_size);
#endif


    // Setup splitter args
    filedata_t *filedata = malloc(sizeof(filedata_t));
	
    filedata->unit_size = record_length; 
    filedata->fpos = 0;
    filedata->flen = finfo.st_size;
    filedata->fhandle = fd;
	filedata->f_data=fdata;

	
	/*
	printf("unit size: %d \n",filedata->unit_size);
	printf("file position: %d \n",filedata->fpos);
	printf("file length: %d \n",(int)(filedata->flen));
	printf("fhandle: %d \n",filedata->fhandle);
	*/
    CHECK_ERROR (map_reduce_init ());
	
	
	return filedata;
}

/** Finalizes the library, closes open file
 * and unmaps input data allocated in memory
 *
 */ 

void unload_op(filedata_t *fd){

	//printf("unload 1 \n");
	CHECK_ERROR (map_reduce_finalize ());
	//printf("unload 2 \n");
	CHECK_ERROR(close(fd->fhandle) < 0);
	//printf("unload 3 \n"); 
#ifndef NO_MMAP
    CHECK_ERROR(munmap(fd->f_data, fd->flen + 1) < 0);
// #else
//     free (fd);
#endif
	//printf("unload 4 \n");
  
	 //write to file
	//finalize phoenix
	//free memory
}
