#include "../BF/linux/BF.h"
#include "hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int* Block_ReadInts(void *blockptr, int numToRead){
    int *intptr = (int *)blockptr;  // Cast void* to int*
    int *arrayOfInts;
    int j = 0;

    /* Read integers from block*/
    arrayOfInts = malloc(sizeof(int)*numToRead);
    for (j = 0; j < numToRead; j++){
        printf("The int is %d", *(intptr+j));
        memcpy(&arrayOfInts[j], (intptr+j), sizeof(int));
    }
    return arrayOfInts;
}

Record* Block_ReadRecords(void *blockptr, int numToRead){
    Record *recordptr = (int *)blockptr;  // Cast void* to int*
    Record *arrayOfRecords;
    int j = 0;

    /* Read integers from block*/
    arrayOfRecords = malloc(sizeof(int)*numToRead);
    for (j = 0; j < numToRead; j++){
        , *(recordptr+j));
        memcpy(&arrayOfRecords[j], (recordptr+j), sizeof(int));
    }
    return arrayOfRecords;
}




void printDebug(int fileDesc) {
    BlockInfo tempInfo;
    void *block;
    int temp, offset = 0, block_num = 1;

    /* Read block with num 1 */
    if (BF_ReadBlock(fileDesc, block_num, &block) < 0) {
        BF_PrintError("Error at printDebug, when getting block: ");
        return;
    }

    printf("Block %d: \n", block_num);
    memcpy(&tempInfo, block, sizeof(BlockInfo));
    printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.nextOverflowBlock);
    offset += sizeof(BlockInfo);

    while (offset < BLOCK_SIZE) {
        memcpy(&temp, block + offset, sizeof(int));
        offset += sizeof(int);
        printf("%d\n", temp);
    }

    while (tempInfo.nextOverflowBlock != -1) {
        block_num = tempInfo.nextOverflowBlock;

        /* Read next block */
        if (BF_ReadBlock(fileDesc, block_num, &block) < 0) {
            BF_PrintError("Error at printDebug, when getting block: ");
            return;
        }

        printf("\nBlock %d\n", block_num);
        offset = 0;
        memcpy(&tempInfo, block, sizeof(BlockInfo));
        printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.nextOverflowBlock);
        offset += sizeof(BlockInfo);

        while (offset < BLOCK_SIZE) {
            memcpy(&temp, block + offset, sizeof(int));
            offset += sizeof(int);
            printf("%d\n", temp);
        }
    }
}

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
    int i, j;
    int blockCounter = 0;
    int hashTableBlocks = 0;
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

    for (j = 0; j <= buckets+1; j++) { /* We need buckets+2 blocks. Block 0,1 is the Info block and hash table block*/
        /* Allocate a block for the hash table */
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;  //what will happen here ????
        }
    }
    //printf("Block counter is %d", BF_GetBlockCounter(fileDesc));

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

    /* Write the block with num 0 back to BF file */
    if (BF_WriteBlock(fileDesc, 0) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        return -1;
    }

    /* Calculate how many blocks we need for the hash table */
    hashTableBlocks = (buckets*sizeof(int) + sizeof(BlockInfo)) / BLOCK_SIZE ;
    if (((buckets*sizeof(int) + sizeof(BlockInfo)) % BLOCK_SIZE) != 0 ) {  // if we have mod != 0 add 1 more block
        hashTableBlocks += 1;
    }
    //printf("We need : %d for hash table\n", hashTableBlocks);

    /* Read block with num 1 */
    if (BF_ReadBlock(fileDesc, 1, &block) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block: ");
        return -1;
    }

    /* Write first 512 (or bluckets*sizeof(int)) bytes in block 1 */
    int offset = 0;
    int num = 0;
    int numWriten = 0;
    BlockInfo *blockInfo;

    if ((blockInfo = malloc(sizeof(BlockInfo))) == NULL ){
        fprintf(stderr,"Not enough memory\n");
        return -1;
    }

    /* We first write the BlockInfo struct , then the nums*/
    offset += sizeof(BlockInfo);
    for (j = 0; j < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && j < buckets ; j++){  // Write the inecies for blocks
        num = 2 + j;  // Block 2 is the first block for records
        memcpy(block+offset, &num, sizeof(int));
        offset += sizeof(int);
        numWriten++;
    }
    /* Update block info struct */
    blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
    blockInfo->bytesInBlock = numWriten*sizeof(int) + sizeof(BlockInfo);
    if (hashTableBlocks > 1){
        blockInfo->nextOverflowBlock = BF_GetBlockCounter(fileDesc);
    }
    //printf("Bloxck info %d", blockInfo->bytesInBlock);
    memcpy(block, blockInfo, sizeof(BlockInfo));  // Write block info

    /* Write back block 1 */
    if (BF_WriteBlock(fileDesc, 1) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        return -1;
    }

    /* Write the leftover nums in overflow blocks that we will allocate */
    for (j = 1; j < hashTableBlocks; j++){  // we already have 1 block allocated for hash table
        int numWritenInloop = 0;
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;  //what will happen here ????
        }
        blockCounter = BF_GetBlockCounter(fileDesc);

        /* Read overflow block */
        if (BF_ReadBlock(fileDesc, blockCounter-1, &block) < 0) {
            BF_PrintError("Error at CreateIndex, when getting block: ");
            return -1;
        }

        /* Write to overflow block */
        offset = 0 + sizeof(BlockInfo);
        for (i = 0; i < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && i < buckets - numWriten ; i++){  // Write the inecies for blocks
            num = numWriten + 1 + i;  // Block 2 is the first block for records
            memcpy(block+offset, &num, sizeof(int));
            offset += sizeof(int);
            numWritenInloop ++;
        }
        numWriten += numWritenInloop;  // update numWriten for the next loop

        /* Update block info struct */
        blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->nextOverflowBlock = blockCounter + 1;
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));  // Write block info
        //printf("Bloxck info %d", blockInfo->bytesInBlock);

        /* Write back overflow block */
        if (BF_WriteBlock(fileDesc, blockCounter-1) < 0){
            BF_PrintError("Error at CreateIndex, when writing block back");
            return -1;
        }
    }
    free(blockInfo);

    printDebug(fileDesc);

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
