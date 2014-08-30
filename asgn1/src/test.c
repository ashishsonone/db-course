#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "header.h"
#include "utils.h"
#include "exsort.h"
#include "equijoin.h"
#include "groupbyagg.h"


#define MAX 30
#define MAX_QUERY_LEN 1000

const char* enumname(Aggfn e){
	switch(e){
		case aggsum: return "aggsum";
					break;
		case aggcount: return "aggcount";
					break;
		case aggavg: return "aggavg";
					break;
		case aggmax: return "aggmax";
					break;
		case aggmin: return "aggmin";
					break;
	}
}

int processJoinQuery(char *st) { 
  char *ch;
  char type[MAX];
  char r1[MAX];
  char r2[MAX];
  char rout[MAX];
  char arg1[MAX];
  char arg2[MAX];
  char proj[MAX][MAX];
  char *projptr[MAX];
  int attrlist1[MAX], attrlist2[MAX];

  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(r1, ch);
  
  ch = strtok(NULL, " ");   
  strcpy(r2, ch);
  
  ch = strtok(NULL, " ");  //GIVING
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);
  
  ch = strtok(NULL, " ");     // ON
  
  int numjoinattrs=0;
  while (ch != NULL) {
	  ch = strtok(NULL, " ");
	  strcpy(arg1, ch);
	  
	  ch = strtok(NULL, " "); // =
	 
	  ch = strtok(NULL, " "); 
	  strcpy(arg2, ch);
	  
	  int a= atoi(arg1);
	  int b= atoi(arg1+2);
	  int c= atoi(arg2);
	  int d= atoi(arg2+2);
	  if(a==1){
		  attrlist1[numjoinattrs] =b;
	  }
	  else{
		  attrlist2[numjoinattrs] =b;
	  }
	  if(c==1){
		  attrlist1[numjoinattrs] =d;
	  }
	  else{
		  attrlist2[numjoinattrs] =d;
	  }
	  numjoinattrs++;
	  
	  ch = strtok(NULL, " ");     // AND or PROJECT
	  
	  if(ch == NULL || !strcmp(ch, "PROJECT")){
	    break;
	  }
	  	  
  }
  
  int projCnt=0;
  while(ch != NULL) {
	  ch = strtok(NULL, " ");
	  
	  if(ch == NULL)
		break;
	  projptr[projCnt]=ch;
	  projCnt++;	  
  }
  
  int i;
  int projlist[projCnt][2];
  for(i=0;i<projCnt;i++){
	  projlist[i][0] = atoi(projptr[i]);;
	  projlist[i][1] = atoi(projptr[i]+2);
		
  }

  return equijoin(r1, r2, rout, numjoinattrs, attrlist1, attrlist2, projCnt, projlist);
}

int processGroubyQuery(char *st) { 
  char *ch;
  char type[MAX];
  char r1[MAX];
  char rout[MAX];

  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(r1, ch);
  
  ch = strtok(NULL, " ");  //GIVING
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);								
  
  ch = strtok(NULL, " ");     // BY	
  
  int numgbattrs=0;
  int gbattrs[MAX];
  while (ch != NULL) {
	  ch = strtok(NULL, " ");
	  
	  if(!strcmp(ch, "AGG")){
			break;
	  }
	  int a= atoi(ch);
	  gbattrs[numgbattrs] =a;
	  numgbattrs++;
  }
  
  //GROUP r1 GIVING rout BY 1 4 AGG  SUM 1 MIN 2
   int numaggs=0, aggattrs[MAX];
   Aggfn aggfns[MAX];
	char agfun[MAX];
  while(ch != NULL) {
	  ch = strtok(NULL, " ");
	  if(ch==NULL)
			break;
	  strcpy(agfun, ch);
	  
	  ch = strtok(NULL, " "); 
	  int a= atoi(ch);
	  aggattrs[numaggs]=a;
	  
	  if(!strcmp(agfun,"SUM")){
		  aggfns[numaggs]= aggsum;
	  }
	  else if(!strcmp(agfun,"MIN")){
		  aggfns[numaggs]= aggmin;
	  }
	  else if(!strcmp(agfun,"MAX")){
		  aggfns[numaggs]= aggmax;
	  }
	  else if(!strcmp(agfun,"AVG")){
		  aggfns[numaggs]= aggavg;
	  }
	  else if(!strcmp(agfun,"COUNT")){
		  aggfns[numaggs]= aggcount;
	  }
	 
	  numaggs++;	  
  }
  
  return groupbyagg(r1, rout, numgbattrs, gbattrs, numaggs, aggattrs, aggfns);

}

/**
 SORT r1 GIVING rout ON 1 -3
 
 sort(char *inputfilename, char *outputfilename, int numattrs, int attributes[], int bufsize)
**/
int processSortQuery(char *st) { 
  char *ch;
  char type[MAX];
  char r1[MAX];
  char rout[MAX];
  // printf("Split \"%s\"\n", st);
  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(r1, ch);
  
  ch = strtok(NULL, " ");  //GIVING
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);								
  
  ch = strtok(NULL, " ");     // ON
  
  int numattrs=0;
  int attributes[MAX];
  while (ch != NULL) {													
	  ch = strtok(NULL, " ");
	  if(ch == NULL)
			break;
	  int a= atoi(ch);
	  attributes[numattrs] =a;
	  numattrs++;
  }
  
  return sort(r1, rout, numattrs, attributes, 8192);
}


/**
 * CREATE rout ATTRS int char 10 double
 * */
 int processCreateQuery(char *st) { 
  char *ch;
  char type[MAX];
  char rout[MAX];
  //printf("Split \"%s\"\n", st);
  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);								
  
  ch = strtok(NULL, " ");     // ATTRS
  
  unsigned int numattrs=0;
  int attributesTypes[MAX][2];                          
  while (ch != NULL) {													
	  ch = strtok(NULL, " ");
	  if(ch == NULL)
			break;
	
	  if(!strcmp(ch,"int")){
		  attributesTypes[numattrs][0]= 1;
		  attributesTypes[numattrs][1]= sizeof(int);
	  }
	  else if(!strcmp(ch,"double")){
		  attributesTypes[numattrs][0]= 2;
		  attributesTypes[numattrs][1]= sizeof(double);
	  }
	  else if(!strcmp(ch,"char")){
		  ch = strtok(NULL, " ");
		  int a= atoi(ch);
		  attributesTypes[numattrs][0]= 3;
		  attributesTypes[numattrs][1]= a;
	  }
	  numattrs++;
  }
  
  return create(rout, numattrs, attributesTypes);
}

/**
 * 
 *  INSERT r1 VALUES  5   Korth 3.2
 * */
 int processInsertQuery(char *st) { 
  char *ch;
  char type[MAX];
  char rout[MAX];
  //printf("Split \"%s\"\n", st);
  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);								
  
  ch = strtok(NULL, " ");     // VALUES
  
  int cnt=0;
  char* values[MAX];                          
  while (ch != NULL) {													
	  ch = strtok(NULL, " ");
	  if(ch == NULL)
			break;
	
	  values[cnt]=(char *) malloc((strlen(ch)*sizeof(char))+1);
	  strcpy(values[cnt], ch);
	  cnt++;
  }
  
  return insert(rout, values);
}

int processDisplayQuery(char *st) { 
  char *ch;
  char type[MAX];
  char rout[MAX];
  //printf("Split \"%s\"\n", st);
  ch = strtok(st, " ");
  strcpy(type, ch);
  
  ch = strtok(NULL, " ");
  strcpy(rout, ch);								
  
  return display(rout);
}

int processQuery(char* query){
  
  //trim newline
  size_t ln = strlen(query) - 1;
  if (query[ln] == '\n')
    query[ln] = '\0';

  char type[10];
  sscanf(query, "%s", type);

  if(!strcmp(type, "JOIN")){
    return processJoinQuery(query);
  }
  else if(!strcmp(type, "GROUP")){
    return processGroubyQuery(query);
  }
  else if(!strcmp(type, "SORT")){
    return processSortQuery(query);
  }
  else if(!strcmp(type, "CREATE")){
    return processCreateQuery(query);
  }
  else if(!strcmp(type, "INSERT")){
    return processInsertQuery(query);
  }
  else if(!strcmp(type, "DISPLAY")){
    return processDisplayQuery(query);
  }
  else{
    printf("Unknown command %s\n", type);
  }

  return 0;
}

int main(){
  
  char query[1000];
  char choice[10];
  int numBytes = 1000;
  int numChoiceBytes = 10;

  while(1){
    printf("Enter query string (or \"quit\" to exit): \n");  
    fgets(query, numBytes, stdin);
    
    if(query != NULL){
      if(strcmp(query, "quit\n")){
	processQuery(query);
      }
      else{
	break;
      }
    }
    else{
      printf("Error in reading query from stdin \n");
    }
  }

  return 0;
}
