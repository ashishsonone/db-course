/*
 * utils.h
 */

#ifndef UTILS_H__
#define UTILS_H_


int display(char *filename);
int create(char *filename, unsigned int noofattr, int attributes[][2]);
int insert(char *filename, char* attr_values[]);
#endif /* UTILS_H_ */
