#include "../BF/linux/BF.h"
#include "exhash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*----------Helping functions-----------*/
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

int* getHashTableFromBlock(EH_info *infoPtr){
    int *hashTable;
    int size = x_to_the_n(2,infoPtr->depth);
    int numsInHashTable = 0;
    void *block;
    BlockInfo tempInfo;
    int offset = 0, block_num;



    hashTable = malloc(sizeof(int)*size);
    /* Read Block 1 */
    if (BF_ReadBlock(infoPtr->fileDesc, 1, &block) < 0) {
        BF_PrintError("Error at getHashTable, when getting block: ");
        return NULL;
    }
    memcpy(&tempInfo, block, sizeof(BlockInfo));
    offset += sizeof(BlockInfo);

    while (offset < tempInfo.bytesInBlock && numsInHashTable < size) {
        memcpy(&hashTable[numsInHashTable], block + offset, sizeof(int));
        offset += sizeof(int);
        numsInHashTable++;
    }

    while (tempInfo.nextOverflowIndex != -1) {
        block_num = tempInfo.nextOverflowIndex;

        /* Read next block */
        if (BF_ReadBlock(infoPtr->fileDesc, block_num, &block) < 0) {
            BF_PrintError("Error at printDebug, when getting block: ");
            return NULL;
        }

        offset = 0;
        memcpy(&tempInfo, block, sizeof(BlockInfo));
        offset += sizeof(BlockInfo);

        while (offset < tempInfo.bytesInBlock && numsInHashTable < size) {
            memcpy(&hashTable[numsInHashTable], block + offset, sizeof(int));
            offset += sizeof(int);
            numsInHashTable++;
        }
    }

    return hashTable;
}

/*-----------end-------------*/

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
    printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\nLocalDepth = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.nextOverflowIndex, tempInfo.localDepth);
    offset += sizeof(BlockInfo);

    /* If its the firs block we read int's , if it's another block then read records */
    if (block_num != 1) {
        flag = 1;
    }

    while (offset < tempInfo.bytesInBlock) {
        if (flag == 1) {
            memcpy(&recordTemp, block + offset, sizeof(Record));
            offset += sizeof(Record);
            printRecord(&recordTemp);
            printf("\n");
        } else {
            memcpy(&intTemp, block + offset, sizeof(int));
            offset += sizeof(int);
            printf("%d\n", intTemp);
        }
    }


    /* Print the overflow Blocks */
    while (tempInfo.nextOverflowIndex != -1) {
        block_num = tempInfo.nextOverflowIndex;

        /* Read next block */
        if (BF_ReadBlock(fileDesc, block_num, &block) < 0) {
            BF_PrintError("Error at printDebug, when getting block: ");
            return;
        }

        printf("\nBlock %d\n", block_num);
        offset = 0;
        memcpy(&tempInfo, block, sizeof(BlockInfo));
        printf("bytesInBlock%d = %d\nnextOverflowBlock = %d\nLocalDepth = %d\n", block_num, tempInfo.bytesInBlock, tempInfo.nextOverflowIndex, tempInfo.localDepth);
        offset += sizeof(BlockInfo);

        while (offset < tempInfo.bytesInBlock) {
            if (flag == 1) {
                memcpy(&recordTemp, block + offset, sizeof(Record));
                offset += sizeof(Record);
                printRecord(&recordTemp);
                printf("\n");
            } else {
                memcpy(&intTemp, block + offset, sizeof(int));
                offset += sizeof(int);
                printf("%d\n", intTemp);
            }

        }
    }
}

int copyBlocks(int srcIndex, int destIndex, int fileDesc, int zeroSrc) {
    void *temp_src, *temp_dest ;

    temp_dest = malloc(BLOCK_SIZE);
    temp_src = malloc(BLOCK_SIZE);
    if (temp_dest == NULL || temp_src == NULL) {
        fprintf(stderr,"Error at copyBLock : when allocating memory\n");
    }

    /* Read dest and src */
    if (BF_ReadBlock(fileDesc,  destIndex, &temp_dest) < 0) {
        BF_PrintError("Error at copyBlock, when getting block: ");
        return -1;
    }
    if (BF_ReadBlock(fileDesc,  srcIndex, &temp_src) < 0) {
        BF_PrintError("Error at copyBlock, when getting block: ");
        return -1;
    }

    /* Make the copy */
    memcpy(temp_dest, temp_src, BLOCK_SIZE);
    if (zeroSrc) {
        /* Initialise with zeros */
        memset(temp_src, 0, BLOCK_SIZE);
    }

    /* Write back blocks */
    if (BF_WriteBlock(fileDesc,  destIndex) < 0) {
        BF_PrintError("Error at copyBlock, when writing block: ");
        return -1;
    }
    if (BF_WriteBlock(fileDesc,  srcIndex) < 0) {
        BF_PrintError("Error at copyBlock, when writing block: ");
        return -1;
    }

    return 0;
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

int hashInt(int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

int doubleHashTable(EH_info *header_info, int blockIndex, Record *collisionRecord, unsigned long hashIndex4CollisionBlock ) {
    int myBlockIndex, offset, j, numWriten, blockCounter;
    int hashTableBlocks ;
    int newBlockIndex;  // Holds the index of the block we will use to insert new records
    int initialLastBlock;
    int oldBuckets = x_to_the_n(2, header_info->depth); /* Buckets before increasing depth */
    unsigned long hashIndex = 0; /* Block index returned by the hash function */
    char *hashKey;
    void *block;
    char zeroArray[BLOCK_SIZE];
    BlockInfo *blockInfo;
    Record record;
    int recordsInArray1 = 0, recordsInArray2 = 0, sizeOftemparray;    /* Counter of written records in each array */
    Record *temp1RecordArray, *temp2RecordArray; /* Arrays of records to store temporary data */

    printf("\n-------IN DOUBLE-------\n");

    /* Initialise with zeros */
    memset(zeroArray, 0, BLOCK_SIZE);

    /* Allocate memory */
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


    /* Allocate one new block ,that we sure need for the
     * hash table (to write ints), or to write new records
     */

    /* Read block 1 to find out where the rest of hastable is */
    if (BF_ReadBlock(header_info->fileDesc, 1, &block) < 0) {
        BF_PrintError("Error at doubleHashTable, when getting block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));
    printf("Block counter before allocating %d and depth %d\n", BF_GetBlockCounter(header_info->fileDesc)-1, header_info->depth);

    /* If block 1 has a pointer then we can give this block for Records
     * and allocate another one to copy the rest of the hashTable there.
     */
    if (blockInfo->nextOverflowIndex != -1) {

        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }

        /* Copy all the blocks that contain the hash table (exept 1) one block forward */
        int back_index = BF_GetBlockCounter(header_info->fileDesc) - 1;  // start from the end
        int end_index = blockInfo->nextOverflowIndex;  // stop coping to where block 1 points
        while (back_index != end_index) {
            /* Read it*/
            if (BF_ReadBlock(header_info->fileDesc, back_index - 1, &block) < 0) {
                BF_PrintError("Error at doubleHashTable, when getting block: ");
                return -1;
            }
            memcpy(blockInfo, block, sizeof(BlockInfo));
            if (blockInfo->nextOverflowIndex != -1) {  // if its not the last before the new block
                blockInfo->nextOverflowIndex++;  // we move them one forward;
                blockInfo->localDepth = 0;
                memcpy(block, blockInfo, sizeof(BlockInfo));
                /* Write it back */
                if (BF_WriteBlock(header_info->fileDesc, back_index) < 0){
                    BF_PrintError("Error at doubleHashTable, when writing block back a");
                    return -1;
                }
            }

            /* Copy it to the block before */
            copyBlocks(back_index - 1, back_index, header_info->fileDesc, 0);
            back_index--;
        }

        /* Read block 1 again to update its pointer */
        if (BF_ReadBlock(header_info->fileDesc, 1, &block) < 0) {
            BF_PrintError("Error at doubleHashTable, when getting block: ");
            return -1;
        }
        memcpy(blockInfo, block, sizeof(BlockInfo));
        newBlockIndex = blockInfo->nextOverflowIndex;  // we move the hast table one block to give this one as a new record block
        blockInfo->nextOverflowIndex++;  // we move them one forward;
        blockInfo->localDepth = 0;
        memcpy(block, blockInfo, sizeof(BlockInfo));
        /* Write it back */
        if (BF_WriteBlock(header_info->fileDesc, 1) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }

        /* Initialize with 0 the block for new records */
        if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
            BF_PrintError("Error at doubleHashTable, when getting block: ");
            return -1;
        }
        memset(block, 0, BLOCK_SIZE);
        /* Write it back */
        if (BF_WriteBlock(header_info->fileDesc, newBlockIndex) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }

    } else {  // in this case we need it for records
        if (BF_AllocateBlock(header_info->fileDesc) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        newBlockIndex = BF_GetBlockCounter(header_info->fileDesc)-1;
    }
    printf("Block for records is %d and blc %d\n", newBlockIndex, BF_GetBlockCounter(header_info->fileDesc)-1);

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
    memcpy(blockInfo, block, sizeof(BlockInfo));
    
    /* Find out how mane overflow block does it have */
    sizeOftemparray = BLOCK_SIZE - sizeof(BlockInfo);
    while (blockInfo->nextOverflowIndex != -1) {
        sizeOftemparray += BLOCK_SIZE - sizeof(BlockInfo);
        /* Read the next block */
        if (BF_ReadBlock(header_info->fileDesc, blockInfo->nextOverflowIndex, &block) < 0) {
            BF_PrintError("Error allocating block: ");
            return -1;
        }
        memcpy(blockInfo, block, sizeof(BlockInfo));
    }

    //printf("size %d\n",sizeOftemparray);
    //printDebug(header_info->fileDesc, blockIndex);
    //printf("\n");
    /**/
    temp1RecordArray = malloc((size_t)sizeOftemparray);
    temp2RecordArray = malloc((size_t)sizeOftemparray);
    if (temp1RecordArray == NULL || temp2RecordArray == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Start again from blockIndex */
    if (BF_ReadBlock(header_info->fileDesc, blockIndex, &block) < 0) {
        BF_PrintError("Error allocating block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));

    /* Hash each record in Block index and the collision record */
   do {
        offset = sizeof(BlockInfo);
        while( offset < (blockInfo->bytesInBlock - sizeof(BlockInfo)) ) {
    
            /* Get the record */
            memcpy(&record, block + offset, sizeof(Record));  // read the record
            printRecord(&record);
            /* Choose the hashing Key*/
            strcpy(hashKey, header_info->attrName);
            if (strcmp(hashKey, "id") == 0)
                hashIndex =(unsigned long) hashInt(record.id);
            else if (strcmp(hashKey, "name") == 0)
                hashIndex = hashStr(record.name);
            else if (strcmp(hashKey, "surname") == 0)
                hashIndex = hashStr(record.surname);
            else if (strcmp(hashKey, "city") == 0)
                hashIndex = hashStr(record.city);
    
    
            /* Keep the depth+1 least significant bits */
            hashIndex = (hashIndex % x_to_the_n(2, header_info->depth + 1));
    
            /* Split the records into two temporary arrays and then write them to blocks */
            if (hashIndex == hashIndex4CollisionBlock - 2) {
                memcpy(&temp1RecordArray[recordsInArray1], &record, sizeof(Record));
                recordsInArray1++;
            }
            else if (hashIndex == hashIndex4CollisionBlock - 2 + oldBuckets) {
                memcpy(&temp2RecordArray[recordsInArray2], &record, sizeof(Record));
                recordsInArray2++;
            }
            
            /* increase offset */
            offset += sizeof(Record);
            printf("That hashes in index %ld , %d, %ld\n", hashIndex, blockIndex - 2, hashIndex4CollisionBlock);
        }
        
        /* Read the next block if there is one */
       if ( blockInfo->nextOverflowIndex != -1 ) {
           if (BF_ReadBlock(header_info->fileDesc, blockInfo->nextOverflowIndex, &block) < 0) {
               BF_PrintError("Error allocating block: ");
               return -1;
           }
           memcpy(blockInfo, block, sizeof(BlockInfo));
       }
    }  while (blockInfo->nextOverflowIndex != -1);

    printf("Collision Block\n");
    printRecord(collisionRecord);
    printf("\n");

    /* Add the collision record to the write block */
    strcpy(hashKey, header_info->attrName);
    if (strcmp(hashKey, "id") == 0)
        hashIndex = (unsigned long) hashInt(collisionRecord->id);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(collisionRecord->name);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(collisionRecord->surname);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(collisionRecord->city);
    /* Keep the depth+1 least significant bits */
    hashIndex = (hashIndex % x_to_the_n(2, header_info->depth + 1));
    /* Place it to the writhe temp array */
    if (hashIndex == blockIndex - 2) {
        memcpy(&temp1RecordArray[recordsInArray1], collisionRecord, sizeof(Record));
        recordsInArray1++;
    }
    else if (hashIndex == blockIndex - 2 + oldBuckets) {
        memcpy(&temp2RecordArray[recordsInArray2], collisionRecord, sizeof(Record));
        recordsInArray2++;
    }

    /* Debug printing*/
    printf("\nIn temp 1---------\n");
    for (j = 0 ; j < recordsInArray1 ; j++) {
        if (j < recordsInArray1) {
            printRecord(&temp1RecordArray[j]);
            printf("\n");
        }
    }
    printf("\nIn temp 2\n");
    for (j = 0 ; j < recordsInArray2 ; j++) {
        if (j < recordsInArray2) {
            printRecord(&temp2RecordArray[j]);
            printf("\n");
        }
    }
    printf("\n---------\n");
    fflush(stdout);


    /* If records hash in one index only, create overflow block*/
    if ( recordsInArray1 == 0 || recordsInArray2 == 0 ) {
        printf("skatooles");
        /* Update block info - nextOverflowBlock */
        memcpy(blockInfo, block, sizeof(BlockInfo));
        blockInfo->nextOverflowIndex = newBlockIndex;  // used the newly allocated block as the overflow block
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write the block back */
        if (BF_WriteBlock(header_info->fileDesc, blockIndex) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }

        /* Write to the new block the collision block */
        if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
            BF_PrintError("Error at doubleHashTable, when getting block: ");
            return -1;
        }

        memcpy(blockInfo, block, sizeof(BlockInfo));
        memcpy(block, zeroArray, BLOCK_SIZE); /* Overwrite the block's data - Initialise with zeros */

        /* Initialise block info - Initialise localDepth - Write the collision record */
        blockInfo->bytesInBlock = sizeof(BlockInfo) + sizeof(Record);
        blockInfo->localDepth = header_info->depth;
        blockInfo->nextOverflowIndex = -1;
        memcpy(block, blockInfo, sizeof(BlockInfo));
        memcpy(block + sizeof(BlockInfo), collisionRecord, sizeof(Record));

        /* Write the block back */
        if (BF_WriteBlock(header_info->fileDesc, newBlockIndex) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }
        printDebug(header_info->fileDesc,blockIndex);

        return 0;
    }


    /* These 3 vars will help us at writing the temp arrays to blocks */
    int overflowBlockNotNeeded = 0;
    int recordsFitInBLock = (BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(Record);
    int loopCounter = 0;
    int recordsNotWriten = 0;

    /* Write temp1RecordArray to block with index blockIndex */
    if (BF_ReadBlock(header_info->fileDesc, blockIndex, &block) < 0) {
        BF_PrintError("Error allocating block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));  // keep block infos

    offset = 0;
    loopCounter = 0;
    recordsNotWriten = recordsInArray1;
    myBlockIndex = blockIndex;
    do {
        loopCounter++;
        memcpy(block, zeroArray, BLOCK_SIZE); /* Overwrite the block's data - Initialise with zeros */

        /* Update block info - Increase localDepth - Write the records */
        printf("Records in arra %d\n", recordsInArray1);
        blockInfo->localDepth++;
        if (recordsNotWriten > recordsFitInBLock) {
            memcpy(block + sizeof(BlockInfo), &temp1RecordArray[offset], recordsFitInBLock*sizeof(Record));
            recordsNotWriten -= recordsFitInBLock;
            blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsFitInBLock*sizeof(Record);
        } else {
            memcpy(block + sizeof(BlockInfo), &temp1RecordArray[offset], recordsNotWriten*sizeof(Record));
            blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsNotWriten*sizeof(Record);
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write the block back */
        if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }

        /* If no more records to write on the next loop , give the rest overflow blocks to the new block for tempArray2 */
        if (loopCounter * recordsFitInBLock > recordsInArray1) {
            blockInfo->nextOverflowIndex = -1;  // cut its overflow block
            memcpy(block, blockInfo, sizeof(BlockInfo));
            /* Write the block back */
            if (BF_WriteBlock(header_info->fileDesc, blockIndex) < 0){
                BF_PrintError("Error at doubleHashTable, when writing block back a");
                return -1;
            }
            overflowBlockNotNeeded = blockInfo->nextOverflowIndex;
            break;
        }

        /* increase the offset */
        offset += recordsFitInBLock;
        myBlockIndex = blockInfo->nextOverflowIndex;

        /* Read the next block if there is one */
        if ( blockInfo->nextOverflowIndex != -1 ) {
            if (BF_ReadBlock(header_info->fileDesc, blockInfo->nextOverflowIndex, &block) < 0) {
                BF_PrintError("Error allocating block: ");
                return -1;
            }
            memcpy(blockInfo, block, sizeof(BlockInfo));
        }
    } while (blockInfo->nextOverflowIndex != -1) ;

    /* Write temp2RecordArray to block with index newBlockIndex */
    if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
        BF_PrintError("Error allocating block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));  // keep block infos
    blockInfo->nextOverflowIndex = overflowBlockNotNeeded ;  // if its != -1 then we will need this overflow block
                                                             // to write the temp array

    offset = 0;
    loopCounter = 0;
    recordsNotWriten = recordsInArray2;
    myBlockIndex = newBlockIndex;
    do {
        loopCounter++;
        /* If no more records to write , give the rest overflow blocks to the new block for tempArray2 */

        memcpy(block, zeroArray, BLOCK_SIZE); /* Overwrite the block's data - Initialise with zeros */

        /* Update block info - Increase localDepth - Write the records */
        blockInfo->localDepth = header_info->depth +1;
        if (recordsNotWriten > recordsFitInBLock) {
            memcpy(block + sizeof(BlockInfo), &temp2RecordArray[offset], recordsFitInBLock*sizeof(Record));
            recordsNotWriten -= recordsFitInBLock;
            blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsFitInBLock*sizeof(Record);
        } else {
            memcpy(block + sizeof(BlockInfo), &temp2RecordArray[offset], recordsNotWriten*sizeof(Record));
            blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsNotWriten*sizeof(Record);
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));

        /* Write the block back */
        if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back a");
            return -1;
        }

        /* increase the offset */
        offset += recordsFitInBLock;
        myBlockIndex = blockInfo->nextOverflowIndex;

        /* Read the next block if there is one */
        if ( blockInfo->nextOverflowIndex != -1 ) {
            if (BF_ReadBlock(header_info->fileDesc, blockInfo->nextOverflowIndex, &block) < 0) {
                BF_PrintError("Error allocating block: ");
                return -1;
            }
            memcpy(blockInfo, block, sizeof(BlockInfo));
        }
    } while (blockInfo->nextOverflowIndex != -1);


#ifdef nooverflow
    memcpy(block, zeroArray, BLOCK_SIZE); /* Overwrite the block's data - Initialise with zeros */

    /* Update block info - Increase localDepth - Write the records */
    blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsInArray1*sizeof(Record);
    blockInfo->localDepth++;
    blockInfo->nextOverflowIndex = -1;
    memcpy(block, blockInfo, sizeof(BlockInfo));
    memcpy(block + sizeof(BlockInfo), temp1RecordArray, recordsInArray1*sizeof(Record));

    /* Write the block back */
    if (BF_WriteBlock(header_info->fileDesc, blockIndex) < 0){
        BF_PrintError("Error at doubleHashTable, when writing block back a");
        return -1;
    }

    /* Write temp2RecordArray to block with index newBlockIndex */
    if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
        BF_PrintError("Error at doubleHashTable, when getting block: ");
        return -1;
    }

    memcpy(blockInfo, block, sizeof(BlockInfo));
    memcpy(block, zeroArray, BLOCK_SIZE); /* Overwrite the block's data - Initialise with zeros */

    /* Initialise block info - Initialise localDepth - Write the records */
    blockInfo->bytesInBlock = sizeof(BlockInfo) + recordsInArray2*sizeof(Record);
    blockInfo->localDepth = header_info->depth + 1;
    blockInfo->nextOverflowIndex = -1;
    memcpy(block, blockInfo, sizeof(BlockInfo));
    memcpy(block + sizeof(BlockInfo), temp2RecordArray, recordsInArray2*sizeof(Record));

    /* Write the block back */
    if (BF_WriteBlock(header_info->fileDesc, newBlockIndex) < 0){
        BF_PrintError("Error at doubleHashTable, when writing block back a");
        return -1;
    }
#endif

    printf("skata print here\n");
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

    /* The last HashTableValue (which is a block index) is write in the last block */
    initialLastBlock = BF_GetBlockCounter(header_info->fileDesc) - 1;
    myBlockIndex = initialLastBlock;
    if (BF_ReadBlock(header_info->fileDesc, initialLastBlock, &block) < 0) {
        BF_PrintError("Error at doubleHashTable, when getting block: ");
        return -1;
    }
    memcpy(blockInfo, block, sizeof(BlockInfo));

    /* Get the hash Table in memory*/
    int * hashTable;
    hashTable = getHashTableFromBlock(header_info);

    /* Calculate how many more blocks we need for the doubling of the hash table */
    int numsLeftToWrite = (BLOCK_SIZE - blockInfo->bytesInBlock) / sizeof(int);

    if (numsLeftToWrite < oldBuckets) {  // we have to 2^oldDepth more numbers .Do they in the las block ;
        hashTableBlocks = ((oldBuckets - numsLeftToWrite)*sizeof(int) + sizeof(BlockInfo)) / BLOCK_SIZE ;
        if ((((oldBuckets - numsLeftToWrite)*sizeof(int) + sizeof(BlockInfo)) % BLOCK_SIZE) != 0) {  // if we have mod != 0 add 1 more block
            hashTableBlocks += 1;
        }
    }
    else {  // else no need for another Block
        hashTableBlocks = 0;
    }

    /* Write nums to the last block, if that has some space (numLeftToWrite) for ints */
    offset = blockInfo->bytesInBlock;
    numWriten = 0;
    while( numWriten < numsLeftToWrite && numWriten < oldBuckets ) {
        if (numWriten  == hashIndex4CollisionBlock - 2) {  // here goes the new block for records
            memcpy(block + offset, &newBlockIndex, sizeof(int));
        } else {
            memcpy(block + offset, &hashTable[numWriten], sizeof(int));
        }
        offset += sizeof(int);
        numWriten++;
    }

    /* Update block info struct */
    blockInfo->bytesInBlock = numWriten*sizeof(int) + blockInfo->bytesInBlock;
    blockInfo->localDepth = 0;
    blockInfo->nextOverflowIndex = -1;
    if (hashTableBlocks >= 1) {
        blockInfo->nextOverflowIndex = BF_GetBlockCounter(header_info->fileDesc);
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
        offset = sizeof(BlockInfo);

        printf("Block counter befor writing is %d\n", BF_GetBlockCounter(header_info->fileDesc)-1);
        while( numWritenInloop < ((BLOCK_SIZE - sizeof(BlockInfo)) / sizeof(int)) && (numWriten < oldBuckets)) {

            //printf("Block idnex %d %d %d\n", newBlockIndex, blockIndex, numWriten);
            if (numWriten == hashIndex4CollisionBlock - 2) {  // here goes the new block for records
                memcpy(block + offset, &newBlockIndex, sizeof(int));
            } else {
                memcpy(block + offset, &hashTable[numWriten], sizeof(int));
            }
            offset += sizeof(int);
            numWriten++;
            numWritenInloop++;

        }

        /* Update block info struct */
        blockInfo->nextOverflowIndex = -1;  // Default block pointer -1
        blockInfo->localDepth = 0;
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        if (hashTableBlocks > 1 + j) {  // if we have more overflow blocks fix the pointer
            blockInfo->nextOverflowIndex = blockCounter;
        }
        memcpy(block, blockInfo, sizeof(BlockInfo));  // Write block info

        /* Write back overflow block */
        if (BF_WriteBlock(header_info->fileDesc, blockCounter-1) < 0){
            BF_PrintError("Error at doubleHashTable, when writing block back");
            return -1;
        }
    }

    printf("Hash[178] = %d and Hash[178+2^depth] = %d\n", hashTable[178], hashTable[178+oldBuckets]);
    printDebug(header_info->fileDesc, 1);

    /* Update the depth in info header and block 0 */
    header_info->depth++;

    printf("\n-------out of DOUBLE-------\n");

    free(hashTable);
    free(hashKey);
    free(blockInfo);

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
            blockInfo->nextOverflowIndex = -1;  // Default next index -1
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
    blockInfo->nextOverflowIndex = -1;  // Default block pointer -1
    blockInfo->bytesInBlock = numWriten*sizeof(int) + sizeof(BlockInfo);
    blockInfo->localDepth = 0;
    if (hashTableBlocks > 1) {
        blockInfo->nextOverflowIndex = BF_GetBlockCounter(fileDesc);
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
        blockInfo->nextOverflowIndex = -1;  // Default block pointer -1
        blockInfo->bytesInBlock = numWritenInloop*sizeof(int) + sizeof(BlockInfo);
        blockInfo->localDepth = 0;
        if (hashTableBlocks > 1 + j){  // if we have more overflow blocks fix the pointer
            blockInfo->nextOverflowIndex = blockCounter;
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

    printf("In open index we have : %c|%s|%d|%d\n", hash_info_ptr->attrType, hash_info_ptr->attrName, hash_info_ptr->attrLength, hash_info_ptr->depth);

    free(buffer);
    return hash_info_ptr;
} 

int EH_CloseIndex(EH_info* header_info) {

    /*
    int * hashTable = getHashTableFromBlock(header_info);
    int hashval;
    int i;
    for (i = 0 ; i < x_to_the_n(2, header_info->depth) ; i++) {
        hashval = hashTable[i];
        printf("\nHashTable[%d] = %d\nPrinting hashed records\n\n", i, hashval);
        fflush(stdout);
        printDebug(header_info->fileDesc, hashval);
        printf("\n");
    }*/

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
    unsigned long hashIndex = 0; /* The value returned by the hash function */
    unsigned long newHashIndex = 0;
    int myBlockIndex, newBlockIndex, currentBlockIndex, i, offset, hashTableBlocks;
    int buckets = x_to_the_n(2, header_info->depth); /* Initially we have 2^depth buckets */
    int *hashTable;          /* We will bring hashTable to main mem for easier indexing */
    Record tempRecord;
    char notFitInBucket = 0;  /* Flag that shows whether a records fits in a bucket or not */
    int recordsInArray1 = 0, recordsInArray2 = 0; /* Counter of written records in each array */
    Record *temp1RecordArray, *temp2RecordArray;  /* Arrays of records to store temporary data */
    BlockInfo *blockInfo, *tempBlockInfo;
    void *block;

    hashKey = malloc((header_info->attrLength + 1) * sizeof(char));
    if (hashKey == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Chose whether to hash based on an int or a string */
    strcpy(hashKey, header_info->attrName);
    if (strcmp(hashKey, "id") == 0)
        hashIndex = (unsigned  long) hashInt(record.id);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(record.name);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(record.surname);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(record.city);

    /* We can't enter records in the first two buckets */
    hashIndex = (hashIndex % buckets) + 2;

    //printf("\nIn insert\n");
    //printRecord(&record);
    //printf("HashIndex is %ld\n", hashIndex);

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    tempBlockInfo = malloc(sizeof(BlockInfo));
    if (tempBlockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Get hash table in mm*/
    hashTable = getHashTableFromBlock(header_info);
    /* Get the block Index for the record (which is a hashTableValue) */
    myBlockIndex = hashTable[hashIndex-2];
    printf("Hash table values is :%d and hashIndex :%ld\n", myBlockIndex, hashIndex);

    /* Read that block */
    if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
        BF_PrintError("Error at insertEntry, when getting block: ");
        return -1;
    }

    /* Take its info */
    memcpy(blockInfo, block, sizeof(BlockInfo));

    /*
     * Insert the entry in block we hashed.
     * If a block is full then double the size of the table (depth++)
     */

    printf("Its depth is %d and global depth is %d\n", blockInfo->localDepth, header_info->depth);
    //printf("Also in block %d\n", myBlockIndex);
    //printDebug(header_info->fileDesc,myBlockIndex);
    /* Check the block and its overflow blocks*/
    do {
        notFitInBucket = 1;
        if (blockInfo->bytesInBlock + sizeof(Record) <= BLOCK_SIZE) {
            /* Write the record */
            memcpy(block + blockInfo->bytesInBlock, &record, sizeof(Record));

            /* Update the block info */
            blockInfo->bytesInBlock += sizeof(Record);
            memcpy(block, blockInfo, sizeof(BlockInfo));

            /* Write back block */
            if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0) {
                BF_PrintError("Error at insertEntry, when writing block back: ");
                return -1;
            }
            /* It fits in here */
            notFitInBucket = 0;
            break;
        }
        /* Read the next block if there is one */
        if ( blockInfo->nextOverflowIndex != -1 ) {
            if (BF_ReadBlock(header_info->fileDesc, blockInfo->nextOverflowIndex, &block) < 0) {
                BF_PrintError("Error at insertEntry, when getting block: ");
                return -1;
            }
            memcpy(blockInfo, block, sizeof(BlockInfo));
        }
    }while ( blockInfo->nextOverflowIndex != -1 );

    /* If it doesn't fit the split or double*/
    if (notFitInBucket) {
        if (blockInfo->localDepth < header_info->depth) {

            printf("-------IN SPLIT-------\n");
            /* Case where local_depth < global_depth */

            /* Allocate a new block at the end of the file */
            if (BF_AllocateBlock(header_info->fileDesc) < 0) {
                BF_PrintError("Error allocating block: ");
                return -1;
            }
            newBlockIndex = BF_GetBlockCounter(header_info->fileDesc) - 1;

            /* Calculate how many blocks we need to store the hash table */
            hashTableBlocks = (x_to_the_n(2, header_info->depth)*sizeof(int) + sizeof(BlockInfo)) / BLOCK_SIZE;
            if ((x_to_the_n(2, header_info->depth)*sizeof(int) + sizeof(BlockInfo) % BLOCK_SIZE) != 0) {
                hashTableBlocks += 1;
            }
            /* Block1 will not be moved in the following lines */
            hashTableBlocks -= 1;
            
            /* Move each block containing the hash table to their subsequent blocks by one position */
            for (i = 1; i <= hashTableBlocks; i++) {
                copyBlocks(BF_GetBlockCounter(header_info->fileDesc)-1-i,
                           BF_GetBlockCounter(header_info->fileDesc)-i,
                           header_info->fileDesc, 0);
            }

            /* Update newBlockIndex since the empty block has been moved */
            newBlockIndex -= hashTableBlocks;

            /* Update the block pointers - Increase by one except for the last one */
            if (BF_ReadBlock(header_info->fileDesc, 1, &block) < 0) {
                BF_PrintError("Error at insertEntry, when getting block: ");
                return -1;
            }

            currentBlockIndex = 1;
            memcpy(tempBlockInfo, block, sizeof(BlockInfo));
            if (tempBlockInfo->nextOverflowIndex != -1) {
                tempBlockInfo->nextOverflowIndex += 1;
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));
            }

            /* Write back block */
            if (BF_WriteBlock(header_info->fileDesc, 1) < 0){
                BF_PrintError("Error at insertEntry, when writing block back: ");
                return -1;
            }

            while (tempBlockInfo->nextOverflowIndex != -1) {
                currentBlockIndex = tempBlockInfo->nextOverflowIndex;
                if (BF_ReadBlock(header_info->fileDesc, currentBlockIndex, &block) < 0) {
                    BF_PrintError("Error at insertEntry, when getting block: ");
                    return -1;
                }

                memcpy(tempBlockInfo, block, sizeof(BlockInfo));
                if (tempBlockInfo->nextOverflowIndex != -1) {
                    tempBlockInfo->nextOverflowIndex += 1;
                    memcpy(block, tempBlockInfo, sizeof(BlockInfo));
                }

                /* Write back block */
                if (BF_WriteBlock(header_info->fileDesc, currentBlockIndex) < 0) {
                    BF_PrintError("Error at insertEntry, when writing block back: ");
                    return -1;
                }
            }

            /* Add the index of the new block in the hash table */
            if (BLOCK_SIZE - tempBlockInfo->bytesInBlock >= sizeof(int)) {
                memcpy(block + tempBlockInfo->bytesInBlock, &newBlockIndex, sizeof(int));
                tempBlockInfo->bytesInBlock += sizeof(int);
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));

                /* Write back block */
                if (BF_WriteBlock(header_info->fileDesc, currentBlockIndex) < 0) {
                    BF_PrintError("Error at insertEntry, when writing block back: ");
                    return -1;
                }
            }
            else {
                /* Allocate a new block for the hash table at the end of the file */
                if (BF_AllocateBlock(header_info->fileDesc) < 0) {
                    BF_PrintError("Error allocating block: ");
                    return -1;
                }

                tempBlockInfo->nextOverflowIndex = BF_GetBlockCounter(header_info->fileDesc) - 1;
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));
                /* Write back block */
                if (BF_WriteBlock(header_info->fileDesc, currentBlockIndex) < 0) {
                    BF_PrintError("Error at insertEntry, when writing block back: ");
                    return -1;
                }

                /* Read the new block */
                currentBlockIndex = tempBlockInfo->nextOverflowIndex;
                if (BF_ReadBlock(header_info->fileDesc, currentBlockIndex, &block) < 0) {
                    BF_PrintError("Error at insertEntry, when getting block: ");
                    return -1;
                }

                /* Initialise the block */
                tempBlockInfo->bytesInBlock = sizeof(BlockInfo);
                tempBlockInfo->localDepth = 0;
                tempBlockInfo->nextOverflowIndex = -1;
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));
                memcpy(block + tempBlockInfo->bytesInBlock, &newBlockIndex, sizeof(int));
                tempBlockInfo->bytesInBlock += sizeof(int);

                /* Write back block */
                if (BF_WriteBlock(header_info->fileDesc, currentBlockIndex) < 0) {
                    BF_PrintError("Error at insertEntry, when writing block back: ");
                    return -1;
                }
            }

            /* Split the records of the block with index myBlockIndex into two blocks */
            if (BF_ReadBlock(header_info->fileDesc, myBlockIndex, &block) < 0) {
                BF_PrintError("Error at insertEntry, when getting block: ");
                return -1;
            }

            temp1RecordArray = malloc(BLOCK_SIZE - sizeof(BlockInfo));
            temp2RecordArray = malloc(BLOCK_SIZE - sizeof(BlockInfo));
            if (temp1RecordArray == NULL || temp2RecordArray == NULL) {
                printf("Error allocating memory.\n");
                return -1;
            }

            int oldHashIndex = hashIndex;  // change : prosthesa to oldHashIndex
            offset = sizeof(BlockInfo);
            for (; offset < (blockInfo->bytesInBlock - sizeof(BlockInfo)); offset += sizeof(Record)) {
                memcpy(&tempRecord, block + offset, sizeof(Record));

                /* Hash each record again */
                if (strcmp(hashKey, "id") == 0)
                    newHashIndex = (unsigned long)hashInt(tempRecord.id);
                else if (strcmp(hashKey, "name") == 0)
                    newHashIndex = hashStr(tempRecord.name);
                else if (strcmp(hashKey, "surname") == 0)
                    newHashIndex = hashStr(tempRecord.surname);
                else if (strcmp(hashKey, "city") == 0)
                    newHashIndex = hashStr(tempRecord.city);
                newHashIndex += 2;

                /* Keep the depth+1 least significant bits */
                newHashIndex = newHashIndex & ((1 << (blockInfo->localDepth + 1)) - 1);

                // change : debug prints
                printRecord(&tempRecord);
                printf("->That hashes in index %ld\n", newHashIndex);

                /* Split the records into two temporary arrays */
                if (newHashIndex <= oldHashIndex) {  // change : alla3a to blockIndex me to oldHashIndex pou einai to panw hasIndex
                    memcpy(&temp1RecordArray[recordsInArray1], &tempRecord, sizeof(Record));  // change : edw egrafes to record kai oxi tempRecord
                    recordsInArray1++;
                }
                else {
                    memcpy(&temp2RecordArray[recordsInArray2], &tempRecord, sizeof(Record));
                    recordsInArray2++;
                }
            }

            /* Hash the new record */
            if (strcmp(hashKey, "id") == 0)
                newHashIndex = (unsigned long)hashInt(record.id);
            else if (strcmp(hashKey, "name") == 0)
                newHashIndex = hashStr(record.name);
            else if (strcmp(hashKey, "surname") == 0)
                newHashIndex = hashStr(record.surname);
            else if (strcmp(hashKey, "city") == 0)
                newHashIndex = hashStr(record.city);
            newHashIndex += 2;
            /* Keep the depth+1 least significant bits */
            newHashIndex = newHashIndex & ((1 << (blockInfo->localDepth + 1)) - 1);

            /* change : Debug printing*/
            int j ;
            printf("\nIn temp 1---------\n");
            for (j = 0 ; j < recordsInArray1 ; j++) {
                if (j < recordsInArray1) {
                    printRecord(&temp1RecordArray[j]);
                    printf("\n");
                }
            }
            printf("\nIn temp 2\n");
            for (j = 0 ; j < recordsInArray2 ; j++) {
                if (j < recordsInArray2) {
                    printRecord(&temp2RecordArray[j]);
                    printf("\n");
                }
            }
            printf("\n---------\n");
            fflush(stdout);

            /*
             * If array1 has no records and the new record hashes
             * to the new block too then the new block will overflow
             * OR
             * If array2 has no records and the new record hashes
             * to the old block too then the old block will overflow
             */
            if ((recordsInArray1 == 0 && newHashIndex > myBlockIndex) ||
                (recordsInArray2 == 0 && newHashIndex <= myBlockIndex)) {
                /* overflow to newBlockIndex */
            }
            else {
                /* Write temp1RecordArray to block with index myBlockIndex */
                memcpy(tempBlockInfo, block, sizeof(BlockInfo));

                /* Update block info - Increase localDepth - Write the records */
                tempBlockInfo->bytesInBlock = sizeof(BlockInfo) + recordsInArray1*sizeof(Record);
                tempBlockInfo->localDepth = blockInfo->localDepth++;
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));
                memcpy(block + sizeof(BlockInfo), temp1RecordArray, recordsInArray1*sizeof(Record));

                /* Write the block back */
                if (BF_WriteBlock(header_info->fileDesc, myBlockIndex) < 0){
                    BF_PrintError("Error at doubleHashTable, when writing block back a");
                    return -1;
                }

                /* Write temp2RecordArray to block with index newBlockIndex */
                if (BF_ReadBlock(header_info->fileDesc, newBlockIndex, &block) < 0) {
                    BF_PrintError("Error at doubleHashTable, when getting block: ");
                    return -1;
                }

                memcpy(tempBlockInfo, block, sizeof(BlockInfo));

                /* Initialise block info - Initialise localDepth - Write the records */
                tempBlockInfo->bytesInBlock = sizeof(BlockInfo) + recordsInArray2*sizeof(Record);
                tempBlockInfo->localDepth = blockInfo->localDepth++;
                memcpy(block, tempBlockInfo, sizeof(BlockInfo));
                memcpy(block + sizeof(BlockInfo), temp2RecordArray, recordsInArray2*sizeof(Record));

                /* Write the block back */
                if (BF_WriteBlock(header_info->fileDesc, newBlockIndex) < 0){
                    BF_PrintError("Error at doubleHashTable, when writing block back a");
                    return -1;
                }

                printDebug(header_info->fileDesc, myBlockIndex);
                printDebug(header_info->fileDesc, newBlockIndex);
            }
            printf("-------Out of SPLIT-------\n");
        }
        else {
            /* Case where we need to double the hash table in size */
            doubleHashTable(header_info, myBlockIndex, &record, hashIndex);
        }
    }

    //printDebug(header_info->fileDesc, myBlockIndex);
    //printf("\n out of insert \n");

    free(hashTable);
    free(hashKey);
    free(blockInfo);
    free(tempBlockInfo);
    //free(temp1RecordArray);
    //free(temp2RecordArray);

    return 0;
}


int EH_GetAllEntries(EH_info header_info, void *value) {
    char *hashKey, *stringValue;           /* The key passed to the hash function */
    unsigned long hashIndex = 0; /* The value returned by the hash function */
    int myBlockIndex, *intValue, blockCounter = 0, offset, flag = 0, *hashTable;
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
        hashIndex = (unsigned long)hashInt(*intValue);
    else if (strcmp(hashKey, "name") == 0)
        hashIndex = hashStr(stringValue);
    else if (strcmp(hashKey, "surname") == 0)
        hashIndex = hashStr(stringValue);
    else if (strcmp(hashKey, "city") == 0)
        hashIndex = hashStr(stringValue);

    /* We can't enter records in the first two buckets */
    hashIndex = (hashIndex % x_to_the_n(2,header_info.depth)) + 2;

    blockInfo = malloc(sizeof(BlockInfo));
    if (blockInfo == NULL) {
        printf("Error allocating memory.\n");
        return -1;
    }

    /* Look for the record that holds the asked value */
    /* Get hash table in mm*/
    hashTable = getHashTableFromBlock(&header_info);
    /* Get the block Index for the record (which is a hashTableValue) */
    myBlockIndex = hashTable[hashIndex-2];
    printf("In get all entries Hash table values is :%d and hashIndex :%ld\n", myBlockIndex, hashIndex);

    do{
        /* Read Block */
        if (BF_ReadBlock(header_info.fileDesc, myBlockIndex, &block) < 0) {
            BF_PrintError("Error at getAllEntries, when getting block: ");
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
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "name") == 0) {
                if (strcmp(record.name, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "surname") == 0) {
                if (strcmp(record.surname, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    flag = 1;
                }
            }
            else if (strcmp(hashKey, "city") == 0) {
                if (strcmp(record.city, stringValue) == 0) {
                    printf("Record found after searching %d blocks\n",blockCounter);
                    printRecord(&record);
                    flag = 1;
                }
            }

        }

        /* Update myBlockIndex */
        memcpy(blockInfo, block, sizeof(BlockInfo));
        myBlockIndex = blockInfo->nextOverflowIndex;

    }while(myBlockIndex != -1);  // If we found it (flag=1) or no more Blocks then end loop

    if (!flag) {
        printf("We could't find any record with the asked value\n");
    }

    free(hashTable);
    free(blockInfo);
    return 0;

   
}


int HashStatistics(char* filename) {
    /* Add your code here */

    return -1;
   
}
