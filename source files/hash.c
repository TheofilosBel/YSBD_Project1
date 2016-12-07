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
    write2block = malloc((sizeof(char)*(strlen(attrName)+1+1))
                         + sizeof(char)*lengthOfNumber(attrLength)
                         + sizeof(char)*lengthOfNumber(buckets));

    /*make new BF file , and in the first block write the HT info*/
    if ((return_value = BF_CreateFile(fileName)) != 0 )
    {
        BF_PrintError("Error at CreateIndex, when creating file :");
        exit(return_value);
    }
    if ((file_desc = BF_OpenFile(fileName)) < 0)
    {
        BF_PrintError("Error at CreateIndex, when opening file");
        exit(file_desc);
    }
    /* allocate a block to write the HT info */
    if ((return_value = BF_AllocateBlock(file_desc)) < 0)
    {
        BF_PrintError("Error allocating block");
        exit(return_value);
    }
    /* Read block*/
    if ((return_value = BF_ReadBlock(file_desc, 0, &block)) < 0) {
        BF_PrintError("Error at CreateIndex, when getting block");
        exit(return_value);
    }
    /* write the HT info to block*/
    sprintf(write2block,"%c%s%d%d%c", attrType, attrName, attrLength, buckets, endRecord);
    strcpy(block, write2block);
    fprintf(stderr,"Block has inside : %s\n", block);
    /* write back to BF file the block with num 0*/
    if ((return_value = BF_WriteBlock(file_desc, 0)) < 0){
        BF_PrintError("Error at CreateIndex, when writing block back");
        exit(return_value);
    }
    /*Exit with succsess*/
    return 0;
    
}


HT_info* HT_OpenIndex(char *fileName) {
    /* Add your code here */

    return NULL;
    
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
