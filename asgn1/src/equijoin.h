/*
 * equijoin.h
 */

#ifndef EQUIJOIN_H__
#define EQUIJOIN_H_


int equijoin(char*, char*, char*, int, int*, int*, int, int[][2]);

void eclose_files();
void einitbufptr(int i);
int ecompare(const void* a, const void* b);
void ewrite_metadata();
void next1();
void next2();
void ewriteout(void *left, void *right);
void adjust();

#endif /* EQUIJOIN_H_ */
