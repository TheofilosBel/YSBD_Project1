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
    int returnValue = 0; /* Return value of functions - Used to print errors */
    int fileDesc = 0;
    int offset;          /* We need the offset to find in which byte to write to block */
    int j;
    void *block;
    char endRecord = END_RECORD;
    char fieldSeparator = FIELD_SEPARATOR;

    /* Make new BF file and write the HT info in the first block */
    if ((returnValue = BF_CreateFile(fileName)) < 0 ) {
        BF_PrintError("Error at CreateIndex, when creating file: ");
        return -1;
    }
    if ((fileDesc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at CreateIndex, when opening file: ");
        return -1;
    }

    for (j = 0; j <= buckets; j++) { /* We need buckets+1 blocks. Block 0 is the Info block */
        /* Allocate a block for the hash table */
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;  //what will happen here ????
        }
    }

    /* Read block with num 0 */
    if ((returnValue = BF_ReadBlock(fileDesc, 0, &block)) < 0) {
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
    /*
    offset = 0;
    memcpy(block, &attrType, sizeof(char));
    offset += sizeof(char);
    memcpy(block + offset, &fieldSeparator, sizeof(char));
    offset += sizeof(char);

    memcpy(block + offset, attrName, sizeof(char)*(attrLength+1));
    offset += sizeof(char)*(attrLength+1);
    memcpy(block + offset, &fieldSeparator, sizeof(char));
    offset += sizeof(char);

    memcpy((block + offset), &attrLength, sizeof(int));
    offset += sizeof(int);
    memcpy(block + offset, &fieldSeparator, sizeof(char));
    offset += sizeof(char);

    memcpy((block + offset), &buckets, sizeof(int));
    offset += sizeof(int);
    memcpy(block + offset, &endRecord, sizeof(char));
    offset += sizeof(char);
    */
    /* Write the block with num 0 back to BF file and close it */
    if ((returnValue = BF_WriteBlock(fileDesc, 0)) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        return -1;
    }
    if ((returnValue = BF_CloseFile(fileDesc)) < 0) {
        BF_PrintError("Error at CreateIndex, when closing file");
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

    printf("%c\n%s\n%d\n%d\n", hash_info_ptr->attrType, buffer, hash_info_ptr->attrLength, hash_info_ptr->numBuckets);

    hash_info_ptr->fileDesc = fileDesc;
    hash_info_ptr->numBFiles = 1;
    hash_info_ptr->attrName = malloc((hash_info_ptr->attrLength +1) * sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);
    
    fprintf(stderr, "Block in open :%c|%s|%d|%d$\n", hash_info_ptr->attrType, hash_info_ptr->attrName,
            hash_info_ptr->attrLength, hash_info_ptr->numBuckets );

    /* Close the file */
    if (BF_CloseFile(fileDesc) < 0) {
        BF_PrintError("Error at CreateIndex, when closing file: ");
        return NULL;
    }

    free(buffer);
    return hash_info_ptr;
} 


int HT_CloseIndex( HT_info* header_info ) {


    
    return -1;
    
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
