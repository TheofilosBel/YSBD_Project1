#include "../BF/linux/BF.h"
#include "hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Added this comment */

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
    int return_value = 0, file_desc = 0, j, bytes_of_block0 = 0, offset, *leng;
    void *block;
    char end_Record = endRecord;

    /* malloc enough space for HInfo (<512b)*/
    leng = malloc(sizeof(int));
    memcpy(leng, &attrLength, sizeof(int));
    printf("Mem %d %d", *leng, attrLength);

    bytes_of_block0 = 0;
    /*make new BF file , and in the first block write the HT info*/
    if ((return_value = BF_CreateFile(fileName)) != 0 ) {
        BF_PrintError("Error at CreateIndex, when creating file :");
        exit(return_value);
    }
    if ((file_desc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at CreateIndex, when opening file");
        exit(file_desc);
    }

    for (j = 0; j <= buckets; j++){  //we need buckets+1 blocks . Block 0 is the Info block
        /* allocate a block for hash table */
        if (BF_AllocateBlock(file_desc) < 0) {
            BF_PrintError("Error allocating block");
            exit(-1);  //what will happen here ????
        }
    }

    /* Read block with num 0*/
    if ((return_value = BF_ReadBlock(file_desc, 0, &block)) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block");
        exit(return_value);
    }
    /* write the HT info to block [Fromat : attrType,attrName,length,Buckets]*/
    offset = 0;  //we need offset to find in which byte  to write to block
    printf("Block now is %p\n", block);
    memcpy(block, &attrType, sizeof(char));
    offset += sizeof(char);
    printf("Block now is %p\n", block);
    memcpy(block + offset, attrName, sizeof(char)*(attrLength+1));
    offset += sizeof(char)*(attrLength+1);
    printf("Block now is %p\n", block);
    memcpy((block + offset), &attrLength, sizeof(int));
    printf("Block now is %p and we added %d and offset is %d\n", block, block+offset, offset);
    offset += sizeof(int);
    memcpy((block + offset), &buckets, sizeof(int));
    offset += sizeof(int);
    printf("Block now is %p\n", block);
    memcpy(block + offset, &end_Record, sizeof(char));
    offset += sizeof(char);
    printf("Block now is %p and length %d\n", block, offset);
    fflush(stdout);

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
    /*Exit with succsess*/
    return 0;
    
}


HT_info* HT_OpenIndex(char *fileName) {
    int file_desc;
    void *block;
    char *buffer;
    HT_info *hash_info_ptr ;

    /*malloc space for HasInfo struct*/
    hash_info_ptr = malloc(sizeof(HT_info));
    buffer = malloc(25*sizeof(char));  // maximum size in hash.h record is surname with size 25

    /*Open BF file*/
    if ((file_desc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at OpenIndex, when opening file");
        return NULL;
    }
    /* Read block with num 0*/
    if (BF_ReadBlock(file_desc, 0, &block) < 0) {
        BF_PrintError("Error at OpenIndex, when getting block");
        return NULL;
    }
    /* Take info's from block 0*/
    sscanf(block, "%c%s%d%d$", &(hash_info_ptr->attrType), buffer,
           &(hash_info_ptr->attrLength), &(hash_info_ptr->numBuckets) );
    hash_info_ptr->fileDesc = file_desc;
    hash_info_ptr->numBFiles = 1;
    hash_info_ptr->attrName = malloc(hash_info_ptr->attrLength*sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);

    fprintf(stderr, "Block in open :%c|%s|%d|%d$\n", hash_info_ptr->attrType, hash_info_ptr->attrName,
            hash_info_ptr->attrLength, hash_info_ptr->numBuckets );
    /*Close the file*/
    if (BF_CloseFile(file_desc) < 0) {
        BF_PrintError("Error at CreateIndex, when closing file");
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
