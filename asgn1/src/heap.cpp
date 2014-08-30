#include "header.h"

extern FILE *files[K+1];
extern void *ptrs[K+1];
extern int rec_counter[K+1],rec_total,recsize,blkrec;

int compare(const void*, const void*);
void initbufptr(int);
void writemin(void*);

node heap[K+1];
int heapsize;

int parent(int i)
{
	return i/2;
}

int left(int i)
{
	return 2*i;
}

int right(int i)
{
	return 2*i + 1;
}

void minheapify(int i)
{
	int l=left(i);
	int r=right(i);
	int smallest;
	if(l<=heapsize && compare(heap[l].record,heap[i].record)==-1)
	{
		smallest=l;
	}
	else
	{
		smallest=i;
	}
	if(r<=heapsize && compare(heap[r].record,heap[smallest].record)==-1)
	{
		smallest=r;
	}
	if(smallest!=i)
	{
		node temp=heap[i];
		heap[i]=heap[smallest];
		heap[smallest]=temp;
		minheapify(smallest);
	}
}

void buildminheap(int currK)
{
	for(int i=1;i<=currK;i++)						//Initialise data in heap and heapify!!
	{
		heap[i].record=ptrs[i];
		heap[i].blkno=i;
	}
	heapsize=currK;
	for(int i=heapsize/2;i>=1;i--)
	{
		minheapify(i);
	}
}

node extractmin()
{
	if(heapsize<1)
	{
		printf("Heap underflow!!!\n");
	}
	node min = heap[1];
	heap[1]=heap[heapsize];
	heapsize--;
	minheapify(1);
	return min;
}

void heapinsert(node k)
{
	int i=++heapsize;
	heap[i]=k;
	while(i>1 && compare(heap[parent(i)].record,heap[i].record)==1)
	{
		node temp=heap[i];
		heap[i]=heap[parent(i)];
		heap[parent(i)]=temp;
		i=parent(i);
	}
}

void getminrecheap()
{
	node min=extractmin();
	
	ptrs[min.blkno]+=recsize;
	rec_counter[min.blkno]--;
	rec_total--;
	
	writemin(min.record);
	
	if(rec_counter[min.blkno]==0 && ptrs[min.blkno]!=NULL)
	{
		    initbufptr(min.blkno);
		    rec_counter[min.blkno] = fread(ptrs[min.blkno],recsize,blkrec,files[min.blkno]);
		    rec_total+=rec_counter[min.blkno];
		    if(rec_counter[min.blkno]==0)
		    	ptrs[min.blkno]=NULL;
	}
	
	if(rec_counter!=0)
	{
		node temp;
		temp.record=ptrs[min.blkno];
		temp.blkno=min.blkno;
		heapinsert(temp);
	}
}
