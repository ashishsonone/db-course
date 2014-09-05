/* Part-A Equijoin implementation */

#include "exsort.h"
#include "header.h"
#include "equijoin.h"

#define BUFFSIZE 400
#define DEBUG 0
#define IFBUG if(DEBUG==1){
#define ENDBUG }

//FUNCTIONS defined elsewhere
int** make2dint(int,int);
// 'e' prefix stands for equijoin. Used for equijoin explicitly
FILE *efiles[3];

int eblocksize;
int erecsize[3], // size of record in outrel, rel1, rel2 resp
    eblockrec[3]; // #records accomodate in a correspoding block(buffer)

void *ebuffer, // buffer allocated to equijoin function
     *ebaseptrs[3], // base pointers of corr. block(buffer)
     *eptrs[3]; // current pos in corr. block(buffer)

int **eattributes[3]; //attributes[0] NULL, [1] about rel1, [2] about rel2
                    // [i] is a 2d array of size [n+1]x[3] where n is no of attributes in corresponding relation
                    // Those 3 things will be attribute type, size and offset respectively

int enumjoinattrs;
int *eattrlist1;
int *eattrlist2;

int enumprojattrs;
int **eprojlist;


//variables used for processing merge
int done = false; //if any of the files end, we are done

int capout; //capacity left in out buffer
int total1, total2; //currently how many in corr buffers
int curr1, curr2; //currently the record in buffer that has been processed( if curr1 == total1, its time to refill the block reading from file)
void *temp_ptr; //used when processing right records with same attribute values(duplicates)
                    
unsigned en[3]; //no of attributes in NULL, rel1, rel2

bool adjustment; //whether right relation's buffer was "adjusted", so that duplicate recs for current key come inside buffer

/* Add equijoin code here */
int equijoin(char* rel1, char* rel2, char* outrel, int numjoinattrs, int attrlist1[], int attrlist2[], int numprojattrs, int projlist[][2])
{
    int i, irel;

    //copy join attributes and proj attributes;
    enumjoinattrs = numjoinattrs;
	eattrlist1 = (int*)calloc(enumjoinattrs,sizeof(int));
	eattrlist2 = (int*)calloc(enumjoinattrs,sizeof(int));
	for(int i=0;i<enumjoinattrs;i++){
		eattrlist1[i] = attrlist1[i];
		eattrlist2[i] = attrlist2[i];
        IFBUG printf("eattrlist1 %d : %d , eattrlist2 %d: %d\n", i, eattrlist1[i], i, eattrlist2[i]); ENDBUG
    }

    
    //sort the relations
	char sorted1[100], sorted2[100];
	sprintf(sorted1,"s.%d.jtmp",1); // .jtmp files are temporary files generated during join phase like these 2 sorted relations
	sprintf(sorted2,"s.%d.jtmp",2);
    IFBUG printf("%s : %s\n", sorted1, sorted2); ENDBUG

    int out1 = sort(rel1, sorted1, numjoinattrs, attrlist1, BUFFSIZE);
    int out2 = sort(rel2, sorted2, numjoinattrs, attrlist2, BUFFSIZE);

    if(!(out1 == 0 && out2 == 0)){
        IFBUG printf("equijoin : Something wrong with sorting relations %d, %d\n", out1, out2); ENDBUG
        return 1;
    }
    IFBUG printf("\n Now Merge phase remaining \n"); ENDBUG

	//Initialization
    
    //Open sorted relations
	efiles[1]=fopen(sorted1,"rb");
	if(!efiles[1])
	{
		IFBUG printf("File: %s not found!\n", sorted1); ENDBUG
		return 1;
	}
	efiles[2]=fopen(sorted2,"rb");
	if(!efiles[2])
	{
		IFBUG printf("File: %s not found!\n", sorted1); ENDBUG
		return 1;
	}

	efiles[0]=fopen(outrel,"wb");
	if(!efiles[0])
	{
		IFBUG printf("File: %s not created!\n", outrel); ENDBUG
		return 1;
	}


    eattributes[0] = NULL;
    //store attribute details of rel1 and rel2
    for(irel=1; irel<=2; irel++){
        fread(&en[irel],sizeof(unsigned),1,efiles[irel]);
        eattributes[irel] = make2dint(en[irel]+1, 3);

        IFBUG printf("n : %d\n", en[irel]); ENDBUG

        erecsize[irel] = 0;
        for(i=1; i<=en[irel]; i++){
            int temp;
            fread(eattributes[irel][i],sizeof(int),1,efiles[irel]);			//attributes[0][*] is not used.
            fread(&temp,sizeof(int),1,efiles[irel]);
            eattributes[irel][i][1] = temp;
            eattributes[irel][i][2] = erecsize[irel];
            erecsize[irel] += temp;
            IFBUG printf("attr %d : %d : %d : %d \n", i, eattributes[irel][i][0], eattributes[irel][i][1], eattributes[irel][i][2]); ENDBUG
        }
    }

    //copy project list globally
    if(numprojattrs != 0){
        enumprojattrs = numprojattrs;
        eprojlist = make2dint(enumprojattrs, 2);
        for(int i=0;i<enumprojattrs;i++){
            eprojlist[i][0] = projlist[i][0];
            eprojlist[i][1] = projlist[i][1];
            IFBUG printf("projlist %d : %d , %d\n", i, eprojlist[i][0], eprojlist[i][1]); ENDBUG
        }
    }
    else{
        int index;
        enumprojattrs = en[1] + en[2];
        eprojlist = make2dint(enumprojattrs, 2);
        for(int i=0; i<en[1]; i++){
            eprojlist[i][0] = 1;
            eprojlist[i][1] = i+1;
            IFBUG printf("projlist %d : %d , %d\n", i, eprojlist[i][0], eprojlist[i][1]); ENDBUG
        }
        for(int i=0; i<en[2]; i++){
            index = i+en[1];
            eprojlist[index][0] = 2;
            eprojlist[index][1] = i+1;
            IFBUG printf("projlist %d : %d , %d\n", index, eprojlist[index][0], eprojlist[index][1]); ENDBUG
        }
    }

    IFBUG printf("come out\n"); ENDBUG

    erecsize[0] = 0; //record size of projected output
    for(i=0; i<enumprojattrs; i++){
        int r = eprojlist[i][0]; //relation
        int at = eprojlist[i][1]; //attribute #
        erecsize[0] += eattributes[r][at][1];
        IFBUG printf("r %d, at %d, erecsize %d\n", r, at, erecsize[0]); ENDBUG
    }

    ewrite_metadata();

    //create buffer and init variables
    ebuffer = calloc(BUFFSIZE,sizeof(unsigned char));
    eblocksize = BUFFSIZE/3;
    for(i=0; i<3; i++){
        int bufferoffset;
        eblockrec[i] = eblocksize/erecsize[i];
        bufferoffset = i * eblocksize;
        ebaseptrs[i] = ebuffer + bufferoffset;
        IFBUG printf("rel %d, recsize %d, blocksize %d, blockrec %d, bufferoffset %d\n", i, erecsize[i], eblocksize, eblockrec[i], bufferoffset); ENDBUG
    }

    for(int i=0; i<3; i++){
        einitbufptr(i);
    }

    capout = eblockrec[0]; //no of rec output buffer can hold
    curr1 = -1;
    curr2 = -1;
    total1 = 0;
    total2 = 0;

    next1();
    next2();

    IFBUG printf("Buff1 : #rec %d\n", total1); ENDBUG
    IFBUG printf("Buff2 : #rec %d\n", total2); ENDBUG

    done = false; //always initialize global variables

    adjustment = false;

    while(!done){
        int comp = ecompare(eptrs[1], eptrs[2]);
        if(comp == -1){//left is smaller, so increment it
            next1();
            adjustment = false;
        }
        else if(comp == 1){
            next2();
            adjustment = false;
        }
        else{
            if(adjustment == false){
                adjust();
                IFBUG printf("adjustment called \n"); ENDBUG
                adjustment = true;
            }
            else{
                IFBUG printf("Already adjusted \n"); ENDBUG
            }
            //handle all combinations of left tuple with matching entries in right without actually incrementing
            //right pointer
            int rem = total2-curr2;
            temp_ptr = eptrs[2];

            while(rem > 0){
                int c = ecompare(eptrs[1], temp_ptr);
                if(c != 0) break; //if not joinable, just break
                ewriteout(eptrs[1], temp_ptr);
                temp_ptr += erecsize[2];
                rem--;
            }

            next1(); //only increment the left pointer
        }
    }

    /* testing compare and writeout
    int comp = ecompare(eptrs[1], eptrs[2]);
    IFBUG printf("compare result %d\n", comp); ENDBUG

    ewriteout(eptrs[1], eptrs[2]);
    */

    //write output buffer records remaining to output file
    einitbufptr(0);
    fwrite(eptrs[0], erecsize[0], eblockrec[0]-capout, efiles[0]);

    eclose_files();
    system("rm *.jtmp");										//remove all the join temp files once program is complete.
    printf("Equijoin over\n");
    return 0;
}

void adjust(){
    if(total2 < eblockrec[2]){
        //i.e buffer is not full so no adjustment needed at all
        IFBUG printf("adjust() : buffer not full. so no adjustment needed\n"); ENDBUG
        return;
    }

    //find if records needs to be shifted. This is required if duplicates extend till end of buffer
    int rem = total2 - curr2;
    temp_ptr = eptrs[2];
    while(rem > 0){
        int c = ecompare(eptrs[1], temp_ptr);
        temp_ptr += erecsize[2];
        rem--;
    }

    if(rem > 0){//i.e duplicates doesn't extend till end of buffer, so we're done
        IFBUG printf("adjust() : no adjustment required as such as dupl end before end of buffer\n"); ENDBUG
    }
    else{
        int rem = total2 - curr2;
        IFBUG printf("adjust() : Shifting %d records\n", rem); ENDBUG
	    memcpy(ebaseptrs[2], eptrs[2], rem * erecsize[2]);
        eptrs[2] = ebaseptrs[2] + rem * erecsize[2]; //this is to where rec read from file will be copied

		int t = fread(eptrs[2],erecsize[2],eblockrec[2]-rem,efiles[2]);
        total2 = rem + t;

        curr2 = 0;
        eptrs[2] = ebaseptrs[2];
    }
}

void next1(){
    curr1++;
    if(curr1 == total1){//read next block from file
        einitbufptr(1);
		total1 = fread(eptrs[1],erecsize[1],eblockrec[1],efiles[1]);
        IFBUG printf("next1() : readsize %d, readmax# %d newtotal%d\n", erecsize[1], eblockrec[1], total1); ENDBUG
        curr1 = 0;
        if(total1 == 0){
            done = true; //file has ended
            IFBUG printf("next1() : Relation 1 exausted\n"); ENDBUG
        }
    }
    else{
        IFBUG printf("next1() : incrementing to curr %d/%d\n", curr1, total1); ENDBUG
        eptrs[1] += erecsize[1];
    }
}

void next2(){
    curr2++;
    if(curr2 == total2){//read next block from file
        einitbufptr(2);
		total2 = fread(eptrs[2],erecsize[2],eblockrec[2],efiles[2]);
        IFBUG printf("next2() : readsize %d, readmax# %d newtotal%d\n", erecsize[2], eblockrec[2], total2); ENDBUG
        curr2 = 0;
        if(total2 == 0) {
            IFBUG printf("next2() : Relation 2 exausted\n"); ENDBUG
            done = true; //file has ended
        }
    }
    else{
        IFBUG printf("next2() : incrementing to curr %d/%d\n", curr2, total2); ENDBUG
        eptrs[2] += erecsize[2];
    }
}

void ewriteout(void *left, void *right)
{
	if(capout == 0)
	{
		einitbufptr(0);
        capout = eblockrec[0];
		int temp = fwrite(eptrs[0], erecsize[0], eblockrec[0], efiles[0]);
	}

    for(int i=0; i<enumprojattrs; i++){
        int r = eprojlist[i][0];
        int at = eprojlist[i][1];
        void * rec = (r==1) ? left : right;
        IFBUG printf("ewriteout : memcpy size %d\n", eattributes[r][at][1]); ENDBUG
	    memcpy(eptrs[0],rec + eattributes[r][at][2],eattributes[r][at][1]);
        eptrs[0] += eattributes[r][at][1];
    }
	capout--;
}

void ewrite_metadata(){
    int i;
    fwrite(&enumprojattrs,sizeof(unsigned),1,efiles[0]);
    for(i=0;i<(signed)enumprojattrs;i++)
    {
        int r = eprojlist[i][0];
        int at = eprojlist[i][1];
        fwrite(&eattributes[r][at][0],sizeof(int),1,efiles[0]); //metadata
        fwrite(&eattributes[r][at][1],sizeof(int),1,efiles[0]);
    }
}

void einitbufptr(int i){
    eptrs[i] = ebaseptrs[i];
}

void eclose_files(){
    int i;
    for(i=0; i<3; i++){
        fclose(efiles[i]);
    }
}

//Comparision function.	The heart of this program.
int ecompare(const void* a, const void* b)
{
	int max=enumjoinattrs,param1,param2,offset1,offset2,type1,type2,tempi,sign,ans;
	double tempd;
	
	if(a==NULL)													//If either ptr is null, return the other. If both are null, returns null.
		return 1;
	else if (b==NULL)
		return -1;
	
	for(int i=0;i<max;i++)
	{
		param1 = eattrlist1[i];
		param2 = eattrlist2[i];
		/*if(param<0)
		{
			sign = -1;
			param = -param;
		}
		else
			sign = 1;
        */

        sign = 1;

		type1 = eattributes[1][param1][0];
		offset1 = eattributes[1][param1][2];

		type2 = eattributes[2][param2][0];
		offset2 = eattributes[2][param2][2];
		ans=0;
		switch(type1)
		{
			case 1:								//Integer comparision
				tempi = (*(int*)(a+offset1))-(*(int*)(b+offset2));
                IFBUG printf("ecompare : case1 : diff %d - %d = %d\n",(*(int*)(a+offset1)),(*(int*)(b+offset2)), tempi); ENDBUG
				if(tempi>0)
					ans = sign;
				else if(tempi<0)
					ans = -sign;
				break;
				
			case 2:								//Double comparision
				tempd = (*(double*)(a+offset1))-(*(double*)(b+offset2));
				if(tempd>0.0)
					ans = sign;
				else if(tempd<0.0)
					ans = -sign;
				break;
				
			case 3:								//String comparision
				if(string((char*)(a+offset1))<string((char*)(b+offset2)))
					return -sign;
				else if(string((char*)(a+offset1))>string((char*)(b+offset2)))
					return sign;
		}
		
		if(ans==1||ans==(-1))					//Return only if either is greater than other
			return ans;
		
	}

	return 0;									//Returns 0 finally after deciding that records are equal.
}
