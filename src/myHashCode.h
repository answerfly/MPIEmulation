/************************* 
*** File myHashCode.h 
**************************/  
#ifndef MYHASHCODE_H_INCLUDED  
#define MYHASHCODE_H_INCLUDED  
  
#include <string.h>  
#include"mpi.h"  
#define HASHCODE_MULT 31  
  
//默认的hashCode  
int myHashCodeDefault(void * a);  
  
//int类型hashCode  
int myHashCodeInt(void * a);  
  
//char类型的hashCode  
int myHashCodeChar(void * a);  
  
//string类型的hashCode  
int myHashCodeString(void * a);  

//MPI_Request hashCode
int myHashCodeRequest(void * a);

//MSGL hashCode
int myHashCodeMSGL(void * a);


int myHashCodeMPIComm(void * a);
#endif // MYHASHCODE_H_INCLUDED  
