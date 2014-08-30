#include <stdio.h>
#include <stdlib.h>
#include "utils.h"
#include "string.h"

int display(char *filename){
	int num1;
	int count;
	int attributes[25][2];
	double num2;
	char data2[50];
	FILE *ptr_myfile;
	//Location of bin file
	ptr_myfile=fopen(filename,"rb");
	if (!ptr_myfile)
	{
	  printf("Unable to open file! %s\n", filename);
		return 1;
	}
	int memloc=0;
	unsigned int noofattr;
	int type,length;
	fread(&noofattr,sizeof(unsigned int),1,ptr_myfile);
	printf("No of attributes: %d",noofattr);
	printf("\n\n----------Metadata---------------\n\n");
	for(count=0;count<noofattr;count++){
	  fread(&type,sizeof(int),1,ptr_myfile);
	  fread(&length,sizeof(int),1,ptr_myfile);
		
	  attributes[count][0]=type;
	  attributes[count][1]=length;
	  printf("Attribute %d\ntype: %d length: %d\n\n",count+1,type,length);
	}
	
	int returnval=1;
	while(returnval){
		
		for(count=0;count<noofattr;count++){
			switch (attributes[count][0]){
			case 1: returnval=fread(&num1,attributes[count][1],1,ptr_myfile);
					if(returnval)
					 printf("%d ",num1);
					break;
			case 2: returnval=fread(&num2,attributes[count][1],1,ptr_myfile);
					if(returnval) 
					  printf("%f ",num2);
					break;
			case 3: returnval=fread(data2,attributes[count][1],1,ptr_myfile);
					if(returnval)
					 printf("%s ",data2);
					break;
			}
			
		}
		printf("\n");
	}
	return 0;
}	

int create(char *filename, unsigned int noofattr, int attributes[][2]){
	int num1;
	int count;
	
	double num2;
	char data2[50];
	
	FILE *ptr_myfile;
	//Location of bin file
	ptr_myfile=fopen(filename,"wb");
	if (!ptr_myfile)
	{
		printf("Unable to open file!");
		return 1;
	}
	
	
	int type,length;
	fwrite(&noofattr,sizeof(noofattr),1,ptr_myfile);
	
	
	for(count=0;count<noofattr;count++){
	  type = attributes[count][0];
	  length = attributes[count][1];
	 
	  fwrite(&type,sizeof(int),1,ptr_myfile);
	  fwrite(&length,sizeof(int),1,ptr_myfile);
		
	}
	fclose(ptr_myfile);
	return 0;
	
}

int insert(char *filename, char* attr_values[]){
	int num1;
	int count;
	int attributes[25][2];
	double num2;
	char data2[50];
	FILE *ptr_read,*ptr_append;
	//Location of bin file
	ptr_read=fopen(filename, "rb");
	ptr_append=fopen(filename,"ab");
	if (!ptr_read || !ptr_append)
	{
		printf("Unable to open file!");
		return 1;
	}
	unsigned int noofattr;
	fread(&noofattr,sizeof(unsigned int),1,ptr_read);
	int type,length;
	
	for(count=0;count<noofattr;count++){
		fread(&type,sizeof(int),1,ptr_read);
		fread(&length,sizeof(int),1,ptr_read);
		attributes[count][0]=type;
		attributes[count][1]=length;
	}
	
	  for(count=0;count<noofattr;count++){

		
		if(attributes[count][0]==1){
		
		
		   int val1;
		   val1 = atoi((char*)attr_values[count]);
		   fwrite(&val1,sizeof(val1),1,ptr_append); 
		   }
		else if(attributes[count][0]==2){
		  double val2;
		  val2 = atof( attr_values[count]);
		   fwrite(&val2,sizeof(val2),1,ptr_append);
		   }  
		
		else if(attributes[count][0]==3){
		  char val3[attributes[count][1]];
		  strcpy(val3,attr_values[count]);
		  fwrite(val3,sizeof(val3[0]),sizeof(val3)/sizeof(val3[0]),ptr_append);
		 } 
		
		else
		  printf("\n error in inserting data");
			
	}
		
	fclose(ptr_read);
	fclose(ptr_append);	 
	return 0;
}
