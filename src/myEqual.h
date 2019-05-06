/************************* 
*** File myEqual.h 
**************************/  
#ifndef MYEQUAL_H_INCLUDED  
#define MYEQUAL_H_INCLUDED  
  
//默认的相等的方法  
int myEqualDefault(void * a, void *b);  
  
//int类型相等的方法  
int myEqualInt(void * a, void *b);  
  
//char类型相等的方法  
int myEqualChar(void * a, void *b);  
  
//string类型相等的方法  
int myEqualString(void * a, void *b);  

//MPI_Request equal
int myEqualRequest(void * a, void *b);
  
//MPI_MSGL equal
int myEqualMSGL(void * a, void *b);
#endif // MYEQUAL_H_INCLUDED  
