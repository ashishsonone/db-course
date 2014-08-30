#include "header.h"

FILE *infile, *outfile, *tempfile, *files[K+1];
int **attributes, recsize, maxrec, *sortparams, runs, recs, passes, blksize, blkrec,rec_counter[K+1],rec_total;
void *buffer, *ptrs[K+1]; 									//ptrs 1 to K are in buffers and 0 is out buffer
unsigned n;

int** make2dint(int,int);
int compare(const void*, const void*);
void initbufptr(int);
void openfiles(int, int, int);
void closefiles(int);
void buildminheap(int);
void getminrecheap();

int sort(char *infilename, char *outfilename, int numattrs, int attrs[], int bufsize)
{
	//Initialization
	infile=fopen(infilename,"rb");
	if(!infile)
	{
		printf("File not found!\n");
		return 1;
	}
	outfile=fopen(outfilename,"wb");
	if(!outfile)
	{
		printf("File not created!\n");
		return 1;
	}

	buffer = calloc(bufsize,sizeof(unsigned char));
	
	fread(&n,sizeof(unsigned),1,infile);

	attributes = make2dint(1+(signed)n,3);
	sortparams = (int*)calloc(1+numattrs,sizeof(int));

	sortparams[0] = numattrs;
	for(int i=1;i<=numattrs;i++)
		sortparams[i] = attrs[i-1];
	
	recsize = 0;
	for(int i=1;i<=(signed)n;i++)
	{
		int temp;
		fread(attributes[i],sizeof(int),1,infile);			//attributes[0][*] is not used.
		fread(&temp,sizeof(int),1,infile);
		attributes[i][1] = temp;
		attributes[i][2] = recsize;
		recsize += temp;
	}
	
	maxrec = bufsize/recsize;								//Max records in buffer while internal sort
	
	recs=0;
	int recs_read,recs_reads;
	char tempname[50];
	for(runs=0;;)
	{
		recs_read = fread(buffer,recsize,maxrec,infile);
		//printf("recs = %d runs=%d",recs_read,runs);
		if(runs==0)
		  recs_reads = recs_read;
		if(recs_read==0)
			break;
		recs+=recs_read;
		qsort(buffer,recs_read,recsize,compare);
		sprintf(tempname,"0.%d.tmp",++runs);				//Run number starts from 1. Pass 0 means internal sort.
		tempfile = fopen(tempname,"wb");
		fwrite(buffer,recsize,recs_read,tempfile);
		
		fclose(tempfile);
	}
	
	passes = (int)ceil(log(runs)/log(K));
	if(runs == 1){
		fwrite(&n,sizeof(unsigned),1,outfile);
		for(int i=1;i<=(signed)n;i++)
		{
			fwrite(&attributes[i][0],sizeof(int),1,outfile); //metadata
			fwrite(&attributes[i][1],sizeof(int),1,outfile);
		}
		fwrite(buffer,recsize,recs_reads,outfile);
	}
	//printf("Total of %d runs generated in internal sort. %d passes needed\n recs_read0 = %d",runs,passes,recs_reads);
	
	blksize = bufsize/(K+1);
	blkrec = blksize/recsize; 								//no. of records per block in buffer
	int nextruns, currK, runsleft;

	for(int pass=1;pass<=passes;pass++)
	{
		if(pass==passes)
		{
			fwrite(&n,sizeof(unsigned),1,outfile);
			for(int i=1;i<=(signed)n;i++)
			{
				fwrite(&attributes[i][0],sizeof(int),1,outfile); //metadata
				fwrite(&attributes[i][1],sizeof(int),1,outfile);
			}
		}

		nextruns = (int)ceil(runs/(double)(K));
		runsleft = runs%(K);
		
		for(int subpass=1;subpass<=nextruns;subpass++)
		{			
			if(subpass==nextruns&&runsleft!=0)
				currK = runsleft;							//In last pass take care how many runs reqd.
			else
				currK = K;
			openfiles(pass,currK,subpass);					//open (subpass-1)th group of currK output files of previous pass
			
			for(int i=0;i<=currK;i++)
				initbufptr(i);
			
			rec_total=0;
			rec_counter[0]=blkrec;							//Counter to maximum. Keeps track of free space.
			for(int i=1;i<=currK;i++)						//Other counters to 0. Keeps track of records remaining to process.
				rec_counter[i]= 0;
			
			for(int i=1;i<=currK;i++)						//Fill buffers for to start the subpass.
			{   
				if(rec_counter[i]==0 && ptrs[i]!=NULL)
				{
				    rec_counter[i]= fread(ptrs[i],recsize,blkrec,files[i]);
				    rec_total+=rec_counter[i];
				    if(rec_counter[i]==0)
				    	ptrs[i]=NULL;
				}
			}
			
			buildminheap(currK);							//Initialise data in heap and heapify!!
			
			//Block 0 is output block, rest 1..currK are input blocks.
			while(rec_total!=0)								//Actual work done in this loop extract min and put in output buffer.
				getminrecheap();

			//First thing to do: write remaining recs from output block to file
			initbufptr(0);
			fwrite(ptrs[0],recsize,blkrec-rec_counter[0],files[0]);
			rec_counter[0]=blkrec;							//Btw this is not needed.
			
			closefiles(currK);
			runs-=currK;
		}
		
		runs=nextruns;										//value of runs change after each pass
	}
	
        system("rm *.tmp");										//remove all the temp files once program is complete.
	printf("Sucess!! Total of %d records sorted.\n",recs);
	fclose(infile);
	fclose(outfile);
	return 0;
}

/*
int main()
{
	char infilename[50];
	char outfilename[50];
	int numattrs;
	int *attrs;
	int bufsize;
	
	printf("\n Enter the input filename :");
	scanf(" %s",infilename);

	printf("\n Enter the output filename :");
	scanf(" %s",outfilename);
	
	printf("\n Enter the number of to attributes used for sorting : ");
	scanf(" %d",&numattrs);
	
	attrs=(int*)calloc(numattrs,sizeof(int));
	printf("\n Enter the %d attributes used for sorting :\n ",numattrs);
	for(int i=0;i<numattrs;i++)
		scanf("%d",&attrs[i]);
	
	printf("\n Enter buffer size in bytes : ");
	scanf("%d",&bufsize);
	
	return sort(infilename,outfilename,numattrs,attrs,bufsize);
}
*/
