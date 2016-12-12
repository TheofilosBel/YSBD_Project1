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
    printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.localDepth);
    offset += sizeof(BlockInfo);

    /* If its the firs block we read int's , if it's another block then read records */
    if (block_num != 1) {
        flag = 1;
    }

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

    if ( flag == 1) {  // No block in extended hashing has overflow blocks
        return;
    }
    /* In case of block 1 and the other blocks that keep the table
     * the localDepth in Block info works for them like a next pointer
     */
    while (tempInfo.localDepth != -1) {
        block_num = tempInfo.localDepth;

        /* Read next block */
        if (BF_ReadBlock(fileDesc, block_num, &block) < 0) {
            BF_PrintError("Error at printDebug, when getting block: ");
            return;
        }

        printf("\nBlock %d\n", block_num);
        offset = 0;
        memcpy(&tempInfo, block, sizeof(BlockInfo));
        printf("bytesInBlock%d = %d\nlocalDepth = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.localDepth);
        offset += sizeof(BlockInfo);

        while (offset < BLOCK_SIZE) {
            memcpy(&intTemp, block + offset, sizeof(int));
            offset += sizeof(int);
            printf("%d\n", intTemp);

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

int doubleHashTable(EH_info *header_info, int blockIndex, Record *conflictRecord) {
    int myBlockIndex, indexOffset = 0, lastIndex, offset, index, j, i, numWriten, blockCounter;
    int hashTableBlocks, oldBuckets = x_to_the_n(2, header_info->depth), newBlockIndex;
    int bytesInArray1 = 0 , bytesInArray2 = 0;
    unsigned long hashIndex;
    char *hashKey;
    void *block;
    char zerroArray[BLOCK_SIZE];
    BlockInfo *blockInfo;
    Record record, *temp1RecordArray, *temp2RecordArray;

    memset(zerroArray,0,BLOCK_SIZE);
    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }
    hashKey = malloc((header_info->attrLength + 1) * sizeof(char));
    if (hashKey == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }
    temp1RecordArray = malloc(BLOCK_SIZE - sizeof(BlockInfo));
    temp2RecordArray = malloc(BLOCK_SIZE - sizeof(BlockInfo));
    if (temp1RecordArray == NULL || temp2RecordArray == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Allocate one new block for the new hash table if needed */
    if (BF_AllocateBlock(header_info->fileDesc) < 0) {
        BF_PrintError("Error allocating block: ");
        return -1;
    }
    newBlockIndex = BF_GetBlockCounter(header_info->fileDesc)-1;

    /* Before writing the indices split the records to of blockIndex to 2 block.
     * Assuming depth is the old depth, before adding 1 , and localIndex is the conflict block.
     * A number that has bits [0, depth-1] bits equal to blockIndex
     * and bit dept 0 ( which is actually the num blockIndex )
     * and anther one that has bits [0, depth-1] bits equal to blockIndex
     * and bit dept 1 ( which is actually the num blockIndex + 2^depth ).
     * The pointer to the first num , which the an index on the hash table
     * will be blockIndex.
     * And the pointer to the other num , which the an index on the hash table too
     * will be the new block we allocated
     */

    /* Read block with index = blockIndex */
    if (BF_ReadBlock(header_info->fileDesc, blockIndex, &block) < 0) {
        BF_PrintError("Error allocating block: ");
        return -1;
    }
    /* Update myBlockIndex */
    memcpy(blockInfo, block, sizeof(BlockInfo));

    /* Hash  each record in Block index*/
    offset = sizeof(BlockInfo);
    for (j = 0; j < (blockInfo->bytesInBlock - sizeof(BlockInfo)) / sizeof(int); j++) {
        memcpy(&record, block + offset, sizeof(Record));  // read the record
        printf("In while loop\n");
        printRecord(&record);

        printf("The j is %d\n",j);
        fflush(stdout);
        strcpy(hashKey, header_info->attrName);
        if (strcmp(hashKey, "id") == 0){}  //choose thw
            //hashIndex = hashInt(record.id);
        else if (strcmp(hashKey, "name") == 0)
            hashIndex = hashStr(record.name);
        else if (strcmp(hashKey, "surname") == 0)
            hashIndex = hashStr(record.surname);
        else if (strcmp(hashKey, "city") == 0)
            hashIndex = hashStr(record.city);

        offset += sizeof(Record);
        /* mod it with 2^depth+1 */
        hashIndex = (hashIndex % x_to_the_n(2,header_info->depth + 1));
        /* separate them in two temp arrays and then write them to blocks */
        if ( hashIndex == blockIndex - 2) {  // -2 because of block 0 and block 1
            memcpy(&temp1RecordArray[bytesInArray1], &record, sizeof(Record));
            bytesInArray1++;
        } else if (hashIndex == blockIndex - 2 + oldBuckets) {  // same here
            memcpy(&temp2RecordArray[bytesInArray2], &record, sizeof(Record));
            bytesInArray2 ++;
        }
        printf("That hashes in index %d\n", hashIndex);

    }

    /* Debug printing*/
    printf("In temp 1\n");
    for (j = 0 ; j < bytesInArray1 ; j++) {
        if (j < bytesInArray1) {
            printRecord(&temp1RecordArray[j]);
            printf("\n");
        }
    }
    printf("\nIn temp 2\n");
    for (j = 0 ; j < bytesInArray2 ; j++) {
        if (j < bytesInArray2) {
            printRecord(&temp2RecordArray[j]);
            printf("\n");
        }
    }
    fflush(stdout);

    /*Write temp 1 array to block with index blockIndex */
    memcpy(blockInfo, block, sizeof(BlockInfo));  // get block info's
    memcpy(block, zerroArray, BLOCK_SIZE);  // inisilize the block

    blockInfo->bytesInBlock = sizeof(BlockInfo) + bytesInArray1*sizeof(Record);
    blockInfo->localDepth++;  // add 1 to local depth

    memcpy(block, blockInfo, sizeof(BlockInfo));
    memcpy(block + sizeof(BlockInfo), temp1RecordArray, bytesInArray1*sizeof(Record));  // re-rite it
    if (BF_WriteBlock(header_info->fileDesc, blockIndex) < 0){
        BF_PrintError("Error at doubleHashTable, when writing block back a");
        return -1;
    }


    /* Write temp 2 array to block with index newBlockIndex */
    if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
        BF_PrintError("Error at doubleHashTable, when getting block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));  // get block info's
    memcpy(block, zerroArray, BLOCK_SIZE);  // inisilize the block

    blockInfo->bytesInBlock = sizeof(BlockInfo) + bytesInArray2*sizeof(Record);
    blockInfo->localDepth = header_info->depth + 1;  // it was not initialized
    printf("We took %d %d\n", blockInfo->bytesInBlock, blockInfo->localDepth);

    memcpy(block, blockInfo, sizeof(BlockInfo));
    memcpy(block + sizeof(BlockInfo), temp2RecordArray, bytesInArray2*sizeof(Record));
    if (BF_WriteBlock(header_info->fileDesc, newBlockIndex) < 0){
        BF_PrintError("Error at doubleHashTable, when writing block back a");
        return -1;
    }

    printDebug(header_info->fileDesc, blockIndex);
    printDebug(header_info->fileDesc, newBlockIndex);

    free(temp1RecordArray);
    free(temp2RecordArray);
    fflush(stdout);

    /* Updating the pointers !!! Assuming depth is the old depth
     * Each pointer until 2^depth is the same as before
     * even blockIndex is teh same , only the records where changed
     * After 2^depth every poiner points in the same bucket as the above
     * expet the one that has index 2^depth + blockIndex how now holds
     * the value of the new block allocated.
     * Note :
     * In case of block 1 and the other blocks that keep the table
     * the localDepth in Block info works for them like a next pointer
     */

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

    /* Write nums to the block 1, if that hash some space (numLeftToWrite) for ints */
    offset = blockInfo->bytesInBlock;
    numWriten = lastIndex - 2 + 1;  // the first lastIndex -2 + 1 ints are writen in block 1
    index = 0;
    for (j = 0; j < numsLeftToWrite && j < buckets - numWriten; j++) {
        if (numWriten < oldBuckets + 2){  // write the till we hit oldbuckets
            index = lastIndex + j + 1;
        } else {
            index = j + 2;  // Then start from the beginning
        }

        if (index == blockIndex + oldBuckets) {  // here goes the new block
            index = newBlockIndex;
            j++;
        }
        memcpy(block+offset, &index, sizeof(int));
        offset += sizeof(int);
        numWriten++;
    }
    lastIndex = numWriten + 2 - 1;  // Last int we wrote is

    /* Update block info struct */
    blockInfo->localDepth = -1;  // Local Depth is for these block a pointer
    blockInfo->bytesInBlock = numWriten*sizeof(int) + blockInfo->bytesInBlock;
    if (hashTableBlocks >= 1) {
        blockInfo->localDepth = BF_GetBlockCounter(header_info->fileDesc);
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
        int diffForFirstReach = 0;
        printf("Before i loop numwritis is %d\n", numWriten);
        for (i = 0; i < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && ( buckets > numWriten); i++) {
            if (numWriten < oldBuckets ){  // write the till we hit oldbuckets
                index = lastIndex + 1 + i;
                diffForFirstReach++;
            } else {  // Then start from the beginning
                if (j >= 1) {
                    index = (numWriten + 1 - lastIndex - diffForFirstReach);
                    //printf("Index is %d\n", index);
                } else {
                    index = (numWriten + 1 - lastIndex - diffForFirstReach) + 2;  // Only in the first loop we want to add +2
                    //printf("Index is %d\n", index);
                }
            }
            if (numWriten + 1  == blockIndex + oldBuckets - 2) {  // here goes the new block
                printf("skata2");
                index = newBlockIndex;
            }
            memcpy(block+offset, &index, sizeof(int));
            offset += sizeof(int);
            numWritenInloop ++;
            numWriten++;
        }
        printf("The shit is %d %d\n", i < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) , (i < buckets - numWriten)  );
        printf("Num wireten %d nad buckets %d and i is %d\n",numWriten, buckets, i);

        /* Update block info struct */
        blockInfo->localDepth = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->localDepth = blockCounter;
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
            blockInfo->localDepth = depth;  // Default local depth = global depth
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
    int index = 0;
    int numWriten = 0;

    /* We first write the BlockInfo struct, then the indices of the buckets */
    offset += sizeof(BlockInfo);
    for (j = 0; j < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && j < buckets; j++) {
        index = 2 + j;  // Block 2 is the first block for records
        memcpy(block+offset, &index, sizeof(int));
        offset += sizeof(int);
        numWriten++;
    }

    /* Update block info struct */
    blockInfo->localDepth = -1;  // Default block pointer -1
    blockInfo->bytesInBlock = numWriten*sizeof(int) + sizeof(BlockInfo);
    if (hashTableBlocks > 1) {
        blockInfo->localDepth = BF_GetBlockCounter(fileDesc);
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
            index = numWriten + 2 + i;  // Block 2 is the first block for records
            memcpy(block+offset, &index, sizeof(int));
            offset += sizeof(int);
            numWritenInloop ++;
        }
        numWriten += numWritenInloop;  // update numWriten for the next loop

        /* Update block info struct */
        blockInfo->localDepth = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->localDepth = blockCounter;
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
    myBlockIndex = hashIndex / numsInEachBlock ;  // Because block 1 is the first block with the hash table
    if (hashIndex % numsInEachBlock != 0) {
        myBlockIndex++;
    }
    printf("My block index0 is %d\n", myBlockIndex);
    if ( myBlockIndex > 1 ) {  // if grater than 1 then its in a block at the end of the file
        myBlockIndex-- ;  //remove block 1 from count
        myBlockIndex = buckets + 2 + myBlockIndex - 1;  // its after buckets + 2 , one for block0 and one for block1 and -1 because arithmetic start from 0
    }
    printf("My block index1 is %d, blc %d\n", myBlockIndex, BF_GetBlockCounter(header_info->fileDesc));

    /* Read that block */
    if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
        BF_PrintError("Error at insertEntry, when getting block: ");
        return -1;
    }

    /* Get the block index */
    indexOffset = (hashIndex % numsInEachBlock - 2)*sizeof(int) + sizeof(BlockInfo); // hashIndex starts from the indexOffset byte (-2 -> because numbers start from 2)
    printf("Index offset is %d\n", indexOffset);
    memcpy(&myBlockIndex, block + indexOffset, sizeof(int));
    printf("My block index2 is %d\n", myBlockIndex);
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
        doubleHashTable(header_info, myBlockIndex, &record);


        /* Update nextOverflowBlock index of current block */
        /*
        blockInfo->localDepth = BF_GetBlockCounter(header_info->fileDesc);
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Allocate new overflow block and initialise its info */
        /*
        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        if (BF_ReadBlock(header_info->fileDesc, BF_GetBlockCounter(header_info->fileDesc)-1, &block) < 0) {
            BF_PrintError("Error at insertEntry, when getting block: ");
            return -1;
        }

        blockInfo->bytesInBlock = sizeof(BlockInfo);
        blockInfo->localDepth = -1;
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write the record in the new overflow block */
        /*
        memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));
        */
    }

    //doubleHashTable(header_info, int blockIndex);

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
