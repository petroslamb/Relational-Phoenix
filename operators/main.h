



typedef struct {
	char table;
	int recnum;
	int length;
	char value[10];
	int key;
	char data[80];

}record;


typedef struct {
	char table;
	int recnum;
	int length;
	char value[10];
	char data[80];

}record_new;


void printLoads(filedata_t *);

void printAggr(final_data_t );

void print_srt(final_data_t );

void print_prj(final_data_t );

void print_prt(final_data_t );

void print_sel(final_data_t );
