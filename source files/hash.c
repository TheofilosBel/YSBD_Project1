#include "../BF/linux/BF.h"
#include "hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Computes and returns the length of an integer */
int lengthOfNumber(const int x) {
    if(x>=1000000000) return 10;
    if(x>=100000000) return 9;
    if(x>=10000000) return 8;
    if(x>=1000000) return 7;
    if(x>=100000) return 6;
    if(x>=10000) return 5;
    if(x>=1000) return 4;
    if(x>=100) return 3;
    if(x>=10) return 2;
    return 1;
}


int HT_CreateIndex(char *fileName, char attrType, char* attrName, int attrLength, int buckets) {
    int fileDesc = 0;
    int j;
    void *block;

    /* Make new BF file and write the HT info in the first block */
    if (BF_CreateFile(fileName) < 0 ) {
        BF_PrintError("Error at CreateIndex, when creating file: ");
        return -1;

    }
    if ((fileDesc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at CreateIndex, when opening file: ");
        return -1;
    }

    for (j = 0; j <= buckets+1; j++) { /* We need buckets+1 blocks. Block 0 is the Info block */
        /* Allocate a block for the hash table */
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;  //what will happen here ????
        }
    }

    /* Read block with num 0 */
    if (BF_ReadBlock(fileDesc, 0, &block) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block: ");
        return -1;
    }

    /*
     * Write the HT info to block in the following format:
     * attrType|attrName|attrLength|buckets$
     */
    char buf[256];
    sprintf(buf, "%c|%s|%d|%d$", attrType, attrName, attrLength, buckets);
    memcpy(block, buf, sizeof(buf));

    /* Write the block with num 0 back to BF file and close it */
    if (BF_WriteBlock(fileDesc, 0) < 0){

        BF_PrintError("Error at CreateIndex, when writing block back");
        return -1;
    }

    return 0;
}


HT_info* HT_OpenIndex(char *fileName) {
    int fileDesc;
    void *block;
    char *buffer;
    char *pch; /* Used with strtok() */
    HT_info *hash_info_ptr;

    /* Allocate space for HT_Info struct */
    hash_info_ptr = malloc(sizeof(HT_info));
    if (hash_info_ptr == NULL) {
        fprintf(stderr, "Error allocating memory.\n");
        return NULL;
    }

    /*
     * Maximum size in hash.h record is surname with size 25
     * One extra positions for the '\0' character
     */
    buffer = malloc(26 * sizeof(char));
    if (buffer == NULL) {
        fprintf(stderr, "Error allocating memory.\n");
        return NULL;
    }

    /* Open BF file */
    if ((fileDesc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at OpenIndex, when opening file: ");
        return NULL;
    }

    /* Read block with num 0 */
    if (BF_ReadBlock(fileDesc, 0, &block) < 0) {
        BF_PrintError("Error at OpenIndex, when getting block: ");
        return NULL;
    }

    /* Take info from block 0 */
    pch = strtok(block, "|");
    sscanf(pch, "%c", &(hash_info_ptr->attrType));

    pch = strtok(NULL, "|");
    strcpy(buffer, pch);
    
    pch = strtok(NULL, "|");
    sscanf(pch, "%d", &(hash_info_ptr->attrLength));
    
    pch = strtok(NULL, "$");
    sscanf(pch, "%d", &(hash_info_ptr->numBuckets));

    hash_info_ptr->fileDesc = fileDesc;
    hash_info_ptr->numBFiles = 1;
    hash_info_ptr->attrName = malloc((hash_info_ptr->attrLength +1) * sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);

    free(buffer);
    return hash_info_ptr;
} 


int HT_CloseIndex(HT_info* header_info) {
    if (BF_CloseFile(header_info->fileDesc) < 0) {
        BF_PrintError("Error at CloseIndex, when closing file: \n");
        return -1;
    }

    free(header_info->attrName);
    free(header_info);

    printf("Closed success\n");
    return 0;
}

int HT_InsertEntry(HT_info header_info, Record record) {
    /* Add your code here */
    
    return -1;
    
}



int HT_GetAllEntries(HT_info header_info, void *value) {
    /* Add your code here */

    return -1;

}


int HashStatistics(char* filename) {
    /* Add your code here */
    
    return -1;
    
}
