#ifndef HEADER_H_
#define HEADER_H_

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#define K 7				// K way merge can b tweaked from here.

using namespace std;

typedef struct _node
{
	void* record;
	int blkno;
}node;

typedef enum{aggsum, aggcount, aggavg, aggmax, aggmin} Aggfn;  //cs631-14-Assignment1-partb

#endif /* HEADER_H_ */
