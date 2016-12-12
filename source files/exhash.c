#include "../BF/linux/BF.h"
#include "exhash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


void printRecord(Record* recordptr){
    printf("Record with id :%d\n", recordptr->id);
    printf("Name :%s\nSurname :%s\nCity :%s\n", recordptr->name, recordptr->surname, recordptr->city);
}

int x_to_the_n (int x,int n)
{
    int i; /* Variable used in loop counter */
    int number = 1;

    for (i = 0; i < n; ++i)
        number *= x;

    return(number);
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

    while (offset < BLOCK_SIZE) {
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

        while (offset < BLOCK_SIZE) {
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

/*------------HasH needed Functions-----------------------*/
unsigned long hashStr(char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
}

int doubleHashTable(EH_info *header_info) {
    int myBlockIndex, indexOffset = 0, lastIndex, offset, num, j, i, numWriten, blockCounter;
    int hashTableBlocks, oldBuckets = x_to_the_n(2, header_info->depth);
    void *block;
    BlockInfo *blockInfo;

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Allocate blocks for the new hash table */
    blockCounter = BF_GetBlockCounter(header_info->fileDesc);
    for (j = 0; j < oldBuckets - (blockCounter - oldBuckets - 2); j++) {  // -2 because of block 0 and block 1
        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }

        /* Initialize each block with the BlockInfo struct */
        if (BF_ReadBlock(header_info->fileDesc, BF_GetBlockCounter(header_info->fileDesc) - 1, &block) < 0) {
            BF_PrintError("Error at doubleHashTable, when getting block: ");
            return -1;
        }

        /* Write blockInfo to block */
        blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = sizeof(BlockInfo);  //  bytes are writen to the block
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write back block */
        if (BF_WriteBlock(header_info->fileDesc, BF_GetBlockCounter(header_info->fileDesc) - 1) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back");
            return -1;
        }
    }

    /* Get block Info from block1 that holds the table */
    myBlockIndex = 1;
    if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
        BF_PrintError("Error at doubleHashTable, when getting block: ");
        return -1;
    }

    /* Update myBlockIndex */
    memcpy(blockInfo, block, sizeof(BlockInfo));

    /* Find what was the last Index writen in block 1 */
    indexOffset = blockInfo->bytesInBlock - sizeof(int);  // Last bucket index should be writen in the last int
    memcpy(&lastIndex, block + indexOffset, sizeof(int));

    /* Calculate how many more blocks we need for the doubling of the hash table */
    int numsLeftToWrite = (BLOCK_SIZE - blockInfo->bytesInBlock) / sizeof(int);
    if (numsLeftToWrite < oldBuckets) {
        hashTableBlocks = ((oldBuckets - numsLeftToWrite)*sizeof(int) + sizeof(BlockInfo)) / BLOCK_SIZE ;
        if ((((oldBuckets - numsLeftToWrite)*sizeof(int) + sizeof(BlockInfo)) % BLOCK_SIZE) != 0) {  // if we have mod != 0 add 1 more block
            hashTableBlocks += 1;
        }
    } else {  // else no need for another Block
        hashTableBlocks = 0;
    }

    /* Update the depth in both info header */
    header_info->depth++;
    int buckets = x_to_the_n(2, header_info->depth);

    /* Write nums to the block we read before, that hash some space for numLeftToWrite ints */
    offset = blockInfo->bytesInBlock;
    numWriten = 0;
    for (j = 0; j < numsLeftToWrite && j < buckets - lastIndex; j++) {
        num = lastIndex + 1 + j;  // lastIndex is the last bucket Index for records
        memcpy(block+offset, &num, sizeof(int));
        offset += sizeof(int);
        numWriten++;
    }

    /* Update block info struct */
    blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
    blockInfo->bytesInBlock = numWriten*sizeof(int) + blockInfo->bytesInBlock;
    if (hashTableBlocks >= 1) {
        blockInfo->nextOverflowBlock = BF_GetBlockCounter(header_info->fileDesc);
    }

    /* Write the block info to myBlockIndex */
    memcpy(block, blockInfo, sizeof(BlockInfo));

    /* Write back block myBlockIndex */
    if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0){
        BF_PrintError("Error at doubleHashTable, when writing block back a");
        return -1;
    }

    /* Write the leftover nums in overflow blocks that we will allocate */
    for (j = 0; j < hashTableBlocks; j++) {
        int numWritenInloop = 0;
        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        blockCounter = BF_GetBlockCounter(header_info->fileDesc);

        /* Read overflow block */
        if (BF_ReadBlock(header_info->fileDesc, blockCounter-1, &block) < 0) {
            BF_PrintError("Error at doubleHashTable, when getting block: ");
            return -1;
        }

        /* Write to overflow block */
        offset = 0 + sizeof(BlockInfo);
        for (i = 0; i < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && i < buckets - (numWriten + lastIndex - 2 + 1); i++) {
            num = numWriten + lastIndex + 1 + i;  // Block 2 is the first block for records
            memcpy(block+offset, &num, sizeof(int));
            offset += sizeof(int);
            numWritenInloop ++;
        }
        numWriten += numWritenInloop;  // update numWriten for the next loop
        printf("Num wireten loop %d anf general %d\n",numWritenInloop, numWriten);

        /* Update block info struct */
        blockInfo->nextOverflowBlock = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->nextOverflowBlock = blockCounter;
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));  // Write block info

        /* Write back overflow block */
        if (BF_WriteBlock(header_info->fileDesc, blockCounter-1) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back");
            return -1;
        }
    }

    printDebug(header_info->fileDesc, 1);
    return 1;

}
/*----------------------END----------------------------*/



int EH_CreateIndex(char *fileName, char* attrName, char attrType, int attrLength, int depth) {
    int fileDesc = 0;
    int i, j;
    int blockCounter = 0;
    int buckets = x_to_the_n(2,depth);  // We need 2^depth buckets
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
    sprintf(buf, "%c|%s|%d|%d$", attrType, attrName, attrLength, depth);
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
    printDebug(fileDesc, 1);
    free(blockInfo);
    return 0;
}


EH_info* EH_OpenIndex(char *fileName) {
    int fileDesc;
    void *block;
    char *buffer;
    char *pch; /* Used with strtok() */
    EH_info *hash_info_ptr;

    /* Allocate space for HT_Info struct */
    hash_info_ptr = malloc(sizeof(EH_info));
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
    sscanf(pch, "%d", &(hash_info_ptr->depth));

    hash_info_ptr->fileDesc = fileDesc;
    hash_info_ptr->attrName = malloc((hash_info_ptr->attrLength +1) * sizeof(char));
    strcpy(hash_info_ptr->attrName, buffer);

    printf("In open index we have : %c%s%d%d\n", hash_info_ptr->attrType, hash_info_ptr->attrName, hash_info_ptr->attrLength, hash_info_ptr->depth);

    free(buffer);
    return hash_info_ptr;
} 

int EH_CloseIndex(EH_info* header_info) {
    if (BF_CloseFile(header_info->fileDesc) < 0) {
        BF_PrintError("Error at CloseIndex, when closing file: \n");
        return -1;
    }

    free(header_info->attrName);
    free(header_info);

    printf("Closed success\n");
    return 0;
  
}

int EH_InsertEntry(EH_info* header_info, Record record) {
    char *hashKey;           /* The key passed to the hash function */
    unsigned long hashIndex; /* The value returned by the hash function */
    int myBlockIndex, buckets = x_to_the_n(2,header_info->depth);
    int numsInEachBlock, indexOffset = 0;
    BlockInfo *blockInfo;
    void *block;

    hashKey = malloc((header_info->attrLength + 1) * sizeof(char));
    if (hashKey == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Chose whether to hash based on an int or a string */
    strcpy(hashKey, header_info->attrName);
    if (strcmp(hashKey, "id") == 0){}
        //hashIndex = hashInt(record.id);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(record.name);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(record.surname);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(record.city);

    /* We can't enter records in the first two buckets */
    hashIndex = (hashIndex % buckets) + 2;

    printf("\nIn insert\n");
    printRecord(&record);
    printf("HashIndex is %ld\n", hashIndex);

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Get block in which the hash index is */
    numsInEachBlock = (BLOCK_SIZE-sizeof(BlockInfo)) / sizeof(int);  //how many ints fit in one block
    myBlockIndex = hashIndex / numsInEachBlock + 1;  // Because block 1 is the first block with the hash table
    if ((hashIndex % numsInEachBlock != 0) && (hashIndex % numsInEachBlock != hashIndex)) {
        myBlockIndex++;
    }
    printf("My block index1 is %d", myBlockIndex);

    /* Read that block */
    if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
        BF_PrintError("Error at insertEntry, when getting block: ");
        return -1;
    }

    /* Get the block index */
    indexOffset = (hashIndex % numsInEachBlock - 2)*sizeof(int) + sizeof(BlockInfo); // hashIndex starts from the indexOffset byte (-2 -> because numbers start from 2)
    printf("Index offset is %d", indexOffset);
    memcpy(&myBlockIndex, block + indexOffset, sizeof(int));
    printf("My block index2 is %d", myBlockIndex);
    fflush(stdout);

    /* Gain access to the bucket with index myBlockIndex */
    if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
        BF_PrintError("Error at insertEntry, when getting block: ");
        return -1;
    }

    memcpy(blockInfo, block, sizeof(BlockInfo));

    /*
     * Insert the entry in block we hashed.
     * If a block is full then double the size of the table (depth++)
     */

    printf("Bytes in block%d : %d\n", myBlockIndex, blockInfo->bytesInBlock);

    if (blockInfo->bytesInBlock + sizeof(Record) <= BLOCK_SIZE) {
        /* Write the record */
        memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));

        /* Update the block info */
        blockInfo->bytesInBlock += sizeof(Record);
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write back block */
        if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0){
            BF_PrintError("Error at insertEntry, when writing block back: ");
            return -1;
        }
    }
    else {
        /* Double the hash table */
        doubleHashTable(header_info);


        /* Update nextOverflowBlock index of current block */
        blockInfo->nextOverflowBlock = BF_GetBlockCounter(header_info->fileDesc);
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Allocate new overflow block and initialise its info */
        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        if (BF_ReadBlock(header_info->fileDesc, BF_GetBlockCounter(header_info->fileDesc)-1, &block) < 0) {
            BF_PrintError("Error at insertEntry, when getting block: ");
            return -1;
        }

        blockInfo->bytesInBlock = sizeof(BlockInfo);
        blockInfo->nextOverflowBlock = -1;
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write the record in the new overflow block */
        memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));
    }

    doubleHashTable(header_info);

    //printDebug(header_info->fileDesc, myBlockIndex);

    free(hashKey);
    free(blockInfo);

    return 0;
}


int EH_GetAllEntries(EH_info header_info, void *value) {
    /* Add your code here */

    return -1;
   
}


int HashStatistics(char* filename) {
    /* Add your code here */

    return -1;
   
}
