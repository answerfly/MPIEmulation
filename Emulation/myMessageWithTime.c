#include<stdio.h>
//#include"mpi.h"
#include"myMessageWithTime.h"
//#include<cstdlib>
int getsize(MPI_Datatype datatype)
{

	int size = 0;
	//printf("datatype is %d\n", (int)datatype);
	switch(datatype)
	{		
		case MPI_INT:
	       		size = sizeof(int);
			break;		
		case MPI_CHAR:
			size = sizeof(char);
			break;
		case MPI_SIGNED_CHAR:
		        size = sizeof(signed char);
	      		break;	      
	 	case MPI_UNSIGNED_CHAR:
			size = sizeof(unsigned char);
			break;
		case MPI_BYTE:
			size = 1;
			break;
		case MPI_WCHAR:
		    	size = 2 * sizeof(char);
			break;
		case MPI_SHORT:
			size = sizeof(short);
			break;
		case MPI_UNSIGNED_SHORT:
		        size = sizeof(unsigned short);
       			break;			
		case MPI_UNSIGNED:
		  	size = sizeof(unsigned int);	
			break;
	        case MPI_LONG:
 			size = sizeof(long);
			break;       
		case MPI_UNSIGNED_LONG:
			size = sizeof(unsigned long);
			break;	
		case MPI_FLOAT:
			size = sizeof(float);
			break;         
		case MPI_DOUBLE:
			size = sizeof(double);
			break;       
		case MPI_LONG_DOUBLE: 
			size = sizeof(long double);
			break; 
		//case MPI_LONG_LONG_INT: 
		//	size = sizeof(long long int);
		//	break;
		case MPI_UNSIGNED_LONG_LONG:
			size = sizeof(unsigned long long);
			break;
		case MPI_LONG_LONG:
			size = sizeof(long long);
			break;
		case MPI_DOUBLE_PRECISION:
			size = 8;
			break;
		case MPI_LOGICAL:
			size = 4;
			break;
		case MPI_INTEGER:
			size = 4;
			break;
		case MPI_DOUBLE_COMPLEX: 
			size = 2 * sizeof(double);
			break;
		case MPI_REAL:
			size = 4;
			break;
		default:
			size = 0;	        
	}
	return size;
}

int int2char(int id,char *idString)
{
	if(id==0)
	{
		*idString=0x30;
		//printf("this char is %c\n",0x30);
		return 1;
	}

	char temp[10];
	int i=0;
        while(id>0){
		temp[i]=(id%10+0x30);
		i++;
		id=id/10;
	}
	int len = i;
	char *src = temp+i-1;
	char *dst = idString;
	while(i){
		*dst=*src;
		dst++;
		src--;
		i--;
	}
	return len;
}
int char2int(char *ProcessID_String)
{
	int ProcessID=0;
	char *flag = ProcessID_String;
	while((*flag!='\n')&&(*flag!='\0'))
	{
		ProcessID=ProcessID*10+(*flag-0x30);
		flag++;
	}
        return ProcessID;
}

int IfSameNode(int processNum, int *processList, int number)
{	
	/*for(int i = 0; i<number; i++)
	{
		if(*(processList+i) == processNum)
		  return 1;
	}*/
	return 0;
}

