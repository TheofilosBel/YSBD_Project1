#include "../BF/linux/BF.h"
#include "hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int lengthOfNumber(const int x)
{
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

int HT_CreateIndex( char *fileName, char attrType, char* attrName, int attrLength, int buckets) {
    int return_value = 0, file_desc = 0;
    char *write2block;
    void *block;

    /* malloc enough space for HInfo (<512b)*/
    write2block = malloc((sizeof(char)*(strlen(attrName)+1+1+1+1+1))
                         + sizeof(char)*lengthOfNumber(attrLength)
                         + sizeof(char)*lengthOfNumber(buckets));

    /*make new BF file , and in the first block write the HT info*/
    if ((return_value = BF_CreateFile(fileName)) != 0 ) {
        BF_PrintError("Error at CreateIndex, when creating file :");
        exit(return_value);
    }
    if ((file_desc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at CreateIndex, when opening file");
        exit(file_desc);
    }
    /* allocate a block to write the HT info */
    if ((return_value = BF_AllocateBlock(file_desc)) < 0) {
        BF_PrintError("Error allocating block");
        exit(return_value);
    }
    /* Read block with num 0*/
    if ((return_value = BF_ReadBlock(file_desc, 0, &block)) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block");
        exit(return_value);
    }
    /* write the HT info to block [Fromat : typename length buckets]*/
    sprintf(write2block,"%c%s %d %d%c", attrType, attrName,
                                attrLength, buckets, endRecord);
    strncpy(block, write2block, sizeof(char)*(strlen(write2block)+1));
    fprintf(stderr,"Block has inside : %s\n", block);
    /* write back to BF file the block with num 0 and close it*/
    if ((return_value = BF_WriteBlock(file_desc, 0)) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        exit(return_value);
    }
    if ((return_value = BF_CloseFile(file_desc)) < 0) {
        BF_PrintError("Error at CreateIndex, when closing file");
        exit(return_value);
    }

    free(write2block);
    /*Exit with succsess*/
    return 0;
    
}


HT_info* HT_OpenIndex(char *fileName) {
    int return_value = 0, file_desc;
    void *block;
    char *buffer;
    HT_info *hash_info_ptr ;

    /*malloc space for HasInfo struct*/
    hash_info_ptr = malloc(sizeof(HT_info));
    buffer = malloc(20*sizeof(char));  // maximum size in hash.h record is surname with size 20

    /*Open BF file*/
    if ((file_desc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at OpenIndex, when opening file");
        exit(file_desc);
    }
    /* Read block with num 0*/
    if ((return_value = BF_ReadBlock(file_desc, 0, &block)) < 0) {
        BF_PrintError("Error at OpenIndex, when getting block");
        exit(return_value);
    }
    /* Take info's from block 0*/
    sscanf(block, "%c%s %d %d$", &(hash_info_ptr->attrType), buffer,
           &(hash_info_ptr->attrLength), &(hash_info_ptr->numBuckets) );
    hash_info_ptr->fileDesc = file_desc;
    hash_info_ptr->numBFiles = 1;
    hash_info_ptr->attrName = malloc(hash_info_ptr->attrLength*sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);

    fprintf(stderr, "%c|%s|%d|%d$\n", hash_info_ptr->attrType, hash_info_ptr->attrName,
            hash_info_ptr->attrLength, hash_info_ptr->numBuckets );
    /*Close the file*/
    if ((return_value = BF_CloseFile(file_desc) < 0)) {
        BF_PrintError("Error at CreateIndex, when closing file");
        exit(return_value);
    }

    free(buffer);
    return hash_info_ptr;
    
} 


int HT_CloseIndex( HT_info* header_info ) {
    /* Add your code here */
    
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
