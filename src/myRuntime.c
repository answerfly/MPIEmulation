#include<sys/time.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include"mpi.h"
#include"myMessageWithTime.h"


struct timeval start;
struct timeval stop;
double starttime;
double stoptime;

int MPI_Init(int *argc,char ***argv)
{
	char filename[10];
	int r = PMPI_Init(argc,argv);
	starttime = PMPI_Wtime();
	int r1 = gettimeofday(&start,NULL);
	
	//printf("r1 is %d\n", r1);
	return r;
}


int MPI_Finalize()
{
		//fclose(file);
	    //gettimeofday(&start,NULL);
		stoptime = PMPI_Wtime();
		int r = PMPI_Finalize();

		gettimeofday(&stop,NULL);
		//long time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
		printf("start time is %ld, %ld, stop time is %ld, %ld, runtime is\n", start.tv_sec, start.tv_usec, stop.tv_sec, stop.tv_usec);
		printf("time is %f\n", stoptime - starttime);
		return r;
}


/*int int2char(int id,char *idString)
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
	while(*flag!='\n')
	{
		ProcessID=ProcessID*10+(*flag-0x30);
		flag++;
	}
        return ProcessID;
}*/




