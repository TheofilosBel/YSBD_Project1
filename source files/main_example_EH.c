#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "BF.h"
#include "exhash.h"

#define NAME_SIZE 			15
#define SURNAME_SIZE		20
#define CITY_SIZE           10

#define FILENAME_HASH_ID "hash_id"

int main(int argc, char** argv) {
	EH_info *hashId;
	Record record;

	BF_Init();
	
	/* Create hash index on id */
    if ( EH_CreateIndex(FILENAME_HASH_ID, 'i', "id", sizeof(int), 3) < 0 ) {
		fprintf(stderr, "Error creating hash index.\n");
		exit(EXIT_FAILURE);
	}
	
	/* Open hash index based on id */
	if ((hashId = EH_OpenIndex(FILENAME_HASH_ID)) == NULL) {
		fprintf(stderr, "Error opening hash index.\n");
		EH_CloseIndex(hashId);
		exit(EXIT_FAILURE);
	}

	while (!feof(stdin)) { /* read line, until eof */

		scanf("%d %s %s %c %s %d %c %d %d", &record.id, record.name, record.surname, record.city);
				
		/* Insert record in hash index based on id */
		if (EH_InsertEntry(hashId, record) < 0) {
			fprintf(stderr, "Error inserting entry in hash index\n");
			EH_CloseIndex(hashId);
			exit(EXIT_FAILURE);
		}
	}
	
	/* Close id hash index */
	if (EH_CloseIndex(hashId) < 0) {
		fprintf(stderr, "Error closing id hash index.\n");
		exit(EXIT_FAILURE);
	}
		
	/* Print blocks to see content */ 
		
	return EXIT_SUCCESS;
}
