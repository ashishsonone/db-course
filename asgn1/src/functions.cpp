#include "header.h"

extern FILE *files[K+1], *outfile;
extern int **attributes, *sortparams, blksize, recsize, passes, blkrec;
extern void *buffer, *ptrs[K+1];
extern int rec_counter[K+1];

int** make2dint(int x,int y)
{
	int **ptr = (int**)calloc(x,sizeof(int*));
	for(int i=0;i<x;i++)
		ptr[i] = (int*)calloc(y,sizeof(int));
	return ptr;
}

void openfiles(int p, int k, int sp)
{
	char fname[50];
	if(p==passes)
	{
		files[0]=outfile;										//In last pass, output in outfile.
	}
	else
	{
		sprintf(fname,"%d.%d.tmp",p,sp);
		files[0] = fopen(fname,"wb");
	}
	for(int i=1;i<=k;i++)
	{
		sprintf(fname,"%d.%d.tmp",p-1,i+(sp-1)*K);
		files[i] = fopen(fname,"rb");
		if(!files[i])
		{
			printf("File not found!\n");
			exit(1);
		}
	}
}

void closefiles(int k)
{
	for(int i=0;i<=k;i++)
		fclose(files[i]);
}

void initbufptr(int k)
{
	ptrs[k] = buffer+k*blksize;
}

//Comparision function.	The heart of this program.
int compare(const void* a, const void* b)
{
	int max=sortparams[0],param,offset,type,tempi,sign,ans;
	double tempd;
	
	if(a==NULL)													//If either ptr is null, return the other. If both are null, returns null.
		return 1;
	else if (b==NULL)
		return -1;
	
	for(int i=1;i<=max;i++)
	{
		param = sortparams[i];
		if(param<0)
		{
			sign = -1;
			param = -param;
		}
		else
			sign = 1;
		type = attributes[param][0];
		offset = attributes[param][2];
		ans=0;
		switch(type)
		{
			case 1:								//Integer comparision
				tempi = (*(int*)(a+offset))-(*(int*)(b+offset));
				if(tempi>0)
					ans = sign;
				else if(tempi<0)
					ans = -sign;
				break;
				
			case 2:								//Double comparision
				tempd = (*(double*)(a+offset))-(*(double*)(b+offset));
				if(tempd>0.0)
					ans = sign;
				else if(tempd<0.0)
					ans = -sign;
				break;
				
			case 3:								//String comparision
				if(string((char*)(a+offset))<string((char*)(b+offset)))
					return -sign;
				else if(string((char*)(a+offset))>string((char*)(b+offset)))
					return sign;
		}
		
		if(ans==1||ans==(-1))					//Return only if either is greater than other
			return ans;
		
	}

	return 0;									//Returns 0 finally after deciding that records are equal.
}

void writemin(void *minrec)
{
	if(rec_counter[0]==0)
	{
		initbufptr(0);
		int temp = fwrite(ptrs[0],recsize,blkrec,files[0]);
		rec_counter[0]=blkrec;
	}
	memcpy(ptrs[0],minrec,recsize);
	ptrs[0]+=recsize;
	rec_counter[0]--;
}
