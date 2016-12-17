#include "../BF/linux/BF.h"
#include "hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

void printRecord(Record* recordptr){
    printf("Record with id :%d\n", recordptr->id);
    printf("Name :%s\nSurname :%s\nCity :%s\n", recordptr->name, recordptr->surname, recordptr->city);
}


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
    Record *recordptr = (Record *)blockptr;  // Cast void* to int*
    Record *arrayOfRecords;
    int j = 0;

    /* Read records from block*/
    arrayOfRecords = malloc(sizeof(int)*numToRead);
    for (j = 0; j < numToRead; j++){
        memcpy(&arrayOfRecords[j], (recordptr+j), sizeof(int));
    }

    return arrayOfRecords;
}

unsigned long hashStr(char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
}

unsigned long hashInt(unsigned long x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}


void printDebug(int fileDesc, int blockIndex) {
    BlockInfo tempInfo;
    void *block;
    int offset = 0, block_num = blockIndex, intTemp, flag = 0;
    Record recordTemp;

    /* Read block with num 1 */
    if (BF_ReadBlock(fileDesc, block_num, &block) < 0) {
        BF_PrintError("Error at printDebug, when getting block: ");
        return;
    }

    printf("Block %d: \n", block_num);
    memcpy(&tempInfo, block, sizeof(BlockInfo));
    printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.nextOverflowBlock);
    offset += sizeof(BlockInfo);

    /* If its the firs block we read int's , if it's another block then read records */
    if (block_num != 1) {
        flag = 1;
    }

    while (offset < tempInfo.bytesInBlock) {
        if (flag == 1) {
            memcpy(&recordTemp, block + offset, sizeof(Record));
            printf("Offset is %d", offset);
            offset += sizeof(Record);
            printRecord(&recordTemp);
        } else {
            memcpy(&intTemp, block + offset, sizeof(int));
            offset += sizeof(int);
            printf("%d\n", intTemp);
        }
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

        while (offset < tempInfo.bytesInBlock) {
            if (flag == 1) {
                memcpy(&recordTemp, block + offset, sizeof(Record));
                offset += sizeof(Record);
                printRecord(&recordTemp);
            } else {
                memcpy(&intTemp, block + offset, sizeof(int));
                offset += sizeof(int);
                printf("%d\n", intTemp);
            }
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
    BlockInfo *blockInfo;

    /* Malloc space for blockInfo */
    if ((blockInfo = malloc(sizeof(BlockInfo))) == NULL) {
        fprintf(stderr,"Not enough memory\n");
        return -1;
    }

    /* Make new BF file and write the HT info in the first block */
    if (BF_CreateFile(fileName) < 0 ) {
        BF_PrintError("Error at CreateIndex, when creating file: ");
        return -1;

    }
    if ((fileDesc = BF_OpenFile(fileName)) < 0) {
        BF_PrintError("Error at CreateIndex, when opening file: ");
        return -1;
    }

    /* We need buckets+2 blocks. Block0 is the Info block and Block1 is the hash table block */
    for (j = 0; j <= buckets+1; j++) {
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }

        /* Initialize each block with the BlockInfo struct */
        if (j >= 2){

            /* Read blocks after index 2  */
            if (BF_ReadBlock(fileDesc, j, &block) < 0) {
                BF_PrintError("Error at CreateIndex, when getting block: ");
                return -1;
            }

            /* Write blockInfo to block */
            blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
            blockInfo->bytesInBlock = sizeof(BlockInfo);  //  bytes are writen to the block
            memcpy(block, blockInfo, sizeof(BlockInfo));

            /* Write back block 1 */
            if (BF_WriteBlock(fileDesc, j) < 0){
                BF_PrintError("Error at CreateIndex, when writing block back");
                return -1;
            }
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

    /* Read block with num 1 */
    if (BF_ReadBlock(fileDesc, 1, &block) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block: ");
        return -1;
    }

    /* Write first 512 (or bluckets*sizeof(int)) bytes in block 1 */
    int offset = 0;
    int num = 0;
    int numWriten = 0;

    /* We first write the BlockInfo struct, then the indices of the buckets */
    offset += sizeof(BlockInfo);
    for (j = 0; j < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && j < buckets; j++) {
        num = 2 + j;  // Block 2 is the first block for records
        memcpy(block+offset, &num, sizeof(int));
        offset += sizeof(int);
        numWriten++;
    }

    /* Update block info struct */
    blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
    blockInfo->bytesInBlock = numWriten*sizeof(int) + sizeof(BlockInfo);
    if (hashTableBlocks > 1) {
        blockInfo->nextOverflowBlock = BF_GetBlockCounter(fileDesc);
    }

    /* Write the block info */
    memcpy(block, blockInfo, sizeof(BlockInfo));

    /* Write back block 1 */
    if (BF_WriteBlock(fileDesc, 1) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        return -1;
    }
    /* Write the leftover nums in overflow blocks that we will allocate */
    for (j = 1; j < hashTableBlocks; j++) {  // we already have 1 block allocated for hash table
        int numWritenInloop = 0;
        if (BF_AllocateBlock(fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        blockCounter = BF_GetBlockCounter(fileDesc);

        /* Read overflow block */
        if (BF_ReadBlock(fileDesc, blockCounter-1, &block) < 0) {
            BF_PrintError("Error at CreateIndex, when getting block: ");
            return -1;
        }

        /* Write to overflow block */
        offset = 0 + sizeof(BlockInfo);
        for (i = 0; i < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && i < buckets - numWriten; i++) {
            num = numWriten + 2 + i;  // Block 2 is the first block for records
            memcpy(block+offset, &num, sizeof(int));
            offset += sizeof(int);
            numWritenInloop ++;
        }
        numWriten += numWritenInloop;  // update numWriten for the next loop

        /* Update block info struct */
        blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->nextOverflowBlock = blockCounter;
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));  // Write block info

        /* Write back overflow block */
        if (BF_WriteBlock(fileDesc, blockCounter-1) < 0){
            BF_PrintError("Error at CreateIndex, when writing block back");
            return -1;
        }
    }

    free(blockInfo);
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
    sscanf(pch, "%ld", &(hash_info_ptr->numBuckets));

    hash_info_ptr->fileDesc = fileDesc;
    hash_info_ptr->numBFiles = 1;
    hash_info_ptr->attrName = malloc((hash_info_ptr->attrLength +1) * sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);

    char buf[256];
    sprintf(buf, "%c|%s|%d|%ld$", hash_info_ptr->attrType, buffer, hash_info_ptr->attrLength, hash_info_ptr->numBuckets);
    memcpy(block, buf, sizeof(buf));

    if (BF_WriteBlock(fileDesc, 0) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        return NULL;
    }

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

    return 0;
}

int HT_InsertEntry(HT_info header_info, Record record) {
    char *hashKey;           /* The key passed to the hash function */
    unsigned long hashIndex; /* The value returned by the hash function */
    int myBlockIndex;
    BlockInfo *blockInfo;
    void *block;

    hashKey = malloc((header_info.attrLength + 1) * sizeof(char));
    if (hashKey == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Chose whether to hash based on an int or a string */
    strcpy(hashKey, header_info.attrName);
    if (strcmp(hashKey, "id") == 0)
        hashIndex = hashInt((unsigned long)record.id);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(record.name);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(record.surname);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(record.city);

    /* We can't enter records in the first two buckets */
    hashIndex = (hashIndex % header_info.numBuckets) + 2;

    /* Gain access to the bucket with index hashIndex */
    if (BF_ReadBlock(header_info.fileDesc, hashIndex, &block) < 0) {
        BF_PrintError("Error at insertEntry, when getting block: ");
        return -1;
    }

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    memcpy(blockInfo, block, sizeof(BlockInfo));

    /*
     * Insert the entry in the first block with available space
     * If a block has a nextOverflowBlock index with value
     * not equal to -1 then it can't hold any more records so we pass it
     */
    myBlockIndex = hashIndex;
    while (blockInfo->nextOverflowBlock != -1) {
        myBlockIndex = blockInfo->nextOverflowBlock;
        /* Gain access to the next overflow block */
        if (BF_ReadBlock(header_info.fileDesc, blockInfo->nextOverflowBlock, &block) < 0) {
            BF_PrintError("Error at insertEntry, when getting block: ");
            return -1;
        }
        
        /* Get the block info */
        memcpy(blockInfo, block, sizeof(BlockInfo));
    }

    if (blockInfo->bytesInBlock + sizeof(Record) <= BLOCK_SIZE) {
        /* Write the record */
        memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));
        
        /* Update the block info */
        blockInfo->bytesInBlock += sizeof(Record);
        memcpy(block, blockInfo, sizeof(BlockInfo));
        
        /* Write back block */
        if (BF_WriteBlock(header_info.fileDesc, myBlockIndex) < 0){
            BF_PrintError("Error at insertEntry, when writing block back: ");
            return -1;
        }
    }
    else {
        /* Update nextOverflowBlock index of current block */
        blockInfo->nextOverflowBlock = BF_GetBlockCounter(header_info.fileDesc) - 1;
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write back block */
        if (BF_WriteBlock(header_info.fileDesc, myBlockIndex) < 0){
            BF_PrintError("Error at insertEntry, when writing block back: ");
            return -1;
        }

        /* Allocate new overflow block and initialise its info */
        if (BF_AllocateBlock(header_info.fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        if (BF_ReadBlock(header_info.fileDesc, BF_GetBlockCounter(header_info.fileDesc)-1, &block) < 0) {
            BF_PrintError("Error at insertEntry, when getting block: ");
            return -1;
        }

        blockInfo->bytesInBlock = sizeof(BlockInfo);
        blockInfo->nextOverflowBlock = -1;
        memcpy(block, blockInfo, sizeof(BlockInfo));
    
        /* Write the record in the new overflow block */
        memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));

        /* Write back block */
        if (BF_WriteBlock(header_info.fileDesc, BF_GetBlockCounter(header_info.fileDesc)-1) < 0){
            BF_PrintError("Error at insertEntry, when writing block back: ");
            return -1;
        }
    }

    free(hashKey);
    free(blockInfo);

    return 0;
}



int HT_GetAllEntries(HT_info header_info, void *value) {
    char *hashKey, *stringValue;           /* The key passed to the hash function */
    unsigned long hashIndex; /* The value returned by the hash function */
    int myBlockIndex, *intValue, blockCounter = 0, offset, flag = 0;
    BlockInfo *blockInfo;
    Record record;
    void *block;

    /* Check weather value is an int or a string and cast it */
    if (header_info.attrType == 'c'){
        stringValue = (char *)value;
    } else if (header_info.attrType == 'i') {
        intValue = (int *)value;
    }

    hashKey = malloc((header_info.attrLength + 1) * sizeof(char));
    if (hashKey == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Chose whether to hash based on an int or a string */
    strcpy(hashKey, header_info.attrName);
    if (strcmp(hashKey, "id") == 0)
        hashIndex = hashInt((unsigned long)intValue);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(stringValue);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(stringValue);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(stringValue);

    /* We can't enter records in the first two buckets */
    hashIndex = (hashIndex % header_info.numBuckets) + 2;

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Look for the record that holds the asked value */
    myBlockIndex = hashIndex;
    do{
        /* Read Block */
        if (BF_ReadBlock(header_info.fileDesc, myBlockIndex, &block) < 0) {
            BF_PrintError("Error at insertEntry, when getting block: ");
            return -1;
        }

        /* initialize the vars for the loop */
        offset = sizeof(BlockInfo);
        blockCounter++;

        /* Search each record on the block to find the asked value */
        while ( offset < BLOCK_SIZE ){
            memcpy(&record, block + offset, sizeof(Record));
            offset += sizeof(Record);

            /* Find out which record attribute we need to check */
            if (strcmp(hashKey, "id") == 0){
                if (record.id == *intValue) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    printf("\n");
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "name") == 0) {
                if (strcmp(record.name, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    printf("\n");
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "surname") == 0) {
                if (strcmp(record.surname, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    printf("\n");
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "city") == 0) {
                if (strcmp(record.city, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    printf("\n");
                    flag = 1;
                }
            }

        }

        /* Update myBlockIndex */
        memcpy(blockInfo, block, sizeof(BlockInfo));
        myBlockIndex = blockInfo->nextOverflowBlock;

    }while(myBlockIndex != -1);  // If we found it (flag=1) or no more Blocks then end loop

    if (!flag) {
        printf("We could't find any record with the asked value\n");
    }

    free(blockInfo);
    return 0;

}


int HashStatistics(char* filename) {
    int fileDesc = 0;
    int hashTableBlocks = 0;
    int minRecords = 100000, averageRecords = 0, maxRecords = -1;
    int sumOverflowedBuckets = 0, *overflowBlocksPerBucket;
    int recordsInThisBucket, numBlock;
    void *block;
    BlockInfo *blockInfo;
    HT_info *header_info;

    printf("==================================================\n");
    printf("Hash Statistics\n");
    printf("==================================================\n");

    /* Get the file info */
    if ((header_info = HT_OpenIndex(filename)) == NULL) {
        BF_PrintError("Error at HashStatistics, when opening file: ");
        return -1;
    }

    /* Allocate space for blockInfo */
    if ((blockInfo = malloc(sizeof(BlockInfo))) == NULL) {
        fprintf(stderr,"Not enough memory\n");
        return -1;
    }

    /* Allocate space for overflowBlocksPerBucket and initialise it with zeros */
    overflowBlocksPerBucket = malloc(header_info->numBuckets * sizeof(int));
    if (overflowBlocksPerBucket == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    for (numBlock = 0; numBlock < header_info->numBuckets; numBlock++) {
        overflowBlocksPerBucket[numBlock] = 0;
    }

    /*
     * Calculate the blocks we need to store the hash table 
     * and the number of block indices in the hash table
     */
    /* Read block with num 1 */
    if (BF_ReadBlock(fileDesc, 1, &block) < 0) {
        BF_PrintError("Error at HashStatistics, when getting block: ");
        return -1;
    }

    hashTableBlocks++;
    memcpy(blockInfo, block, sizeof(BlockInfo));
    while (blockInfo->nextOverflowBlock != -1) {
        if (BF_ReadBlock(fileDesc, blockInfo->nextOverflowBlock, &block) < 0) {
            BF_PrintError("Error at HashStatistics, when getting block: ");
            return -1;
        }

        hashTableBlocks++;
        memcpy(blockInfo, block, sizeof(BlockInfo));
    }

    printf("Blocks used to store the info: 1\n");
    printf("Blocks are used to store the hash table: %d\n", hashTableBlocks);
    printf("Blocks are used to store the records: %d\n", BF_GetBlockCounter(header_info->fileDesc)-hashTableBlocks-1);
    printf("Blocks in total: %d\n\n", BF_GetBlockCounter(header_info->fileDesc));

    for (numBlock = 2; numBlock < 2 + header_info->numBuckets; numBlock++) {
        recordsInThisBucket = 0;

        if (BF_ReadBlock(fileDesc, numBlock, &block) < 0) {
            BF_PrintError("Error at HashStatistics, when getting block: ");
            return -1;
        }

        memcpy(blockInfo, block, sizeof(BlockInfo));
        recordsInThisBucket += (blockInfo->bytesInBlock - sizeof(BlockInfo)) / sizeof(Record);

        if (blockInfo->nextOverflowBlock != -1) {
            sumOverflowedBuckets++;
        }

        while (blockInfo->nextOverflowBlock != -1) {
            overflowBlocksPerBucket[numBlock-2]++;
            if (BF_ReadBlock(fileDesc, blockInfo->nextOverflowBlock, &block) < 0) {
                BF_PrintError("Error at HashStatistics, when getting block: ");
                return -1;
            }

            memcpy(blockInfo, block, sizeof(BlockInfo));
            recordsInThisBucket += (blockInfo->bytesInBlock - sizeof(BlockInfo)) / sizeof(Record);
        }

        if (recordsInThisBucket < minRecords) {
            minRecords = recordsInThisBucket;
        }
        if (recordsInThisBucket > maxRecords) {
            maxRecords = recordsInThisBucket;
        }
        averageRecords += recordsInThisBucket;
    }

    averageRecords = averageRecords / BF_GetBlockCounter(header_info->fileDesc);

    printf("Minimum bucket records: %d\n", minRecords);
    printf("Maximum bucket records: %d\n", maxRecords);
    printf("Average bucket records: %d\n\n", averageRecords);
    printf("Average blocks per bucket: %1.1f\n\n", (double)BF_GetBlockCounter(header_info->fileDesc)/header_info->numBuckets);
    printf("Buckets that have overflow blocks: %d\n", sumOverflowedBuckets);

    for (numBlock = 0; numBlock < header_info->numBuckets; numBlock++) {
        if (overflowBlocksPerBucket[numBlock] > 0) {
            if (overflowBlocksPerBucket[numBlock] == 1) {
                printf("Bucket with index %3d has %d overflow block.\n", numBlock+2, overflowBlocksPerBucket[numBlock]);
            }
            else {
                printf("Bucket with index %3d has %d overflow blocks.\n", numBlock+2, overflowBlocksPerBucket[numBlock]);
            }
        }
    }

    free(blockInfo);
    free(overflowBlocksPerBucket);
    return 0;
    
}
