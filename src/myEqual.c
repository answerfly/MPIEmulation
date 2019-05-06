/************************* 
*** File myEqual.c 
**************************/  
#include "myEqual.h"
#include "myMessageWithTime.h"
#include <string.h>  
#include<stdio.h>
  
//默认的相等的方法  
int myEqualDefault(void * a, void *b)  
{  
    
    int *a1 = a;
    int *b1 = b;
    return a == b; 
}   
  
//int类型相等的方法  
int myEqualInt(void * a, void *b)  
{  
    int *aa = (int*) a;  
    int *bb = (int*) b;  
    return *aa == *bb;  
}  
  
//char类型相等的方法  
int myEqualChar(void * a, void *b)  
{  
    char *aa = (char *) a;  
    char *bb = (char *) b;  
    return *aa = *bb;  
}  
  
//string类型相等的方法  
int myEqualString(void * a, void *b)  
{  
    char *aa = (char *) a;  
    char *bb = (char *) b;  
    return strcmp(aa, bb)==0;  
}

//MPI_Request equal
int myEqualRequest(void * a, void *b)  
{  
    int *aa = (int*) a;  
    int *bb = (int *) b;  
    return *aa == *bb;  
}  
  
//MSGL equal
int myEqualMSGL(void * a, void *b)  
{  
    MSGL *aa = (MSGL*) a;  
    MSGL *bb = (MSGL*) b;  
    return (aa->source == bb->source) && (aa->tag == bb->tag) && (aa->comm == bb->comm) && (aa->datatype == bb->datatype);  
}  
