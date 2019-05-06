#include<sys/time.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include"mpi.h"
#include"myMessageWithTime.h"
#include"myHashMap.h"
#include"myHashCode.h"
#include"myEqual.h"
static int IfFileOpen = 0;
//static int IfSelected = 0;
static FILE *file;
MyHashMap *table;
int ProcessID;
const int printID = 0;
//int int2char(int id,char *idString);
//int char2int(char *ProcessID_String);
int MPI_Comm_size(MPI_Comm comm, int *size)
{
	int r = PMPI_Comm_size(comm,size);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"MPI_Comm_size: comm is %d, size is %d\n", comm, *size);
	fputs(buf,file);
	}
	return r;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{
	int r = PMPI_Comm_rank(comm,rank);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"MPI_Comm_rank: comm is %d, rank is %d\n", comm, *rank);
	fputs(buf,file);
	}
	return r;
	
}

int MPI_Comm_split(MPI_Comm comm,int color, int key,MPI_Comm *newcomm)
{
	int r = PMPI_Comm_split(comm,color,key, newcomm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"MPI_Comm_split: source comm is %d, new comm is %d\n", comm, *newcomm);
	fputs(buf,file);
	}
	return r;
	
}

//struct timeval tv;
	//gettimeofday(&tv,NULL);
	//printf("second:%ld\n",tv.tv_sec); 
	//printf("millisecond:%ld\n",tv.tv_sec*1000 + tv.tv_usec/1000);
        //printf("microsecond:%ld\n",tv.tv_sec*1000000 + tv.tv_usec);  

int MPI_Init(int *argc,char ***argv)
{
	char filename[10];
	char ProcessID_String[10];
	int r = PMPI_Init(argc,argv);
	PMPI_Comm_rank(MPI_COMM_WORLD,&ProcessID);
	//printf("processID is %d\n",ProcessID);
	strcpy(filename,"track");
        int len = int2char(ProcessID,ProcessID_String);
	//printf("len is %d\n",len);
        memcpy(filename+3,ProcessID_String,len);
        filename[3+len]='\0';
        //printf("log name is %s\n", filename);	
        if((file=fopen(filename,"w"))!=NULL) 
	{
		IfFileOpen = 1;
	}
	table = createMyHashMap(myHashCodeRequest,myEqualRequest);

	return r;
}

int MPI_Send(void *buf,int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
	//printf("%d: enter send\n", ProcessID);
	int r = PMPI_Send(buf, count,datatype,dest,tag,comm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"send: dest is %d, tag is %d, type is %d, comm is %d\n",dest, tag, datatype,comm);
	fputs(buf,file);
	}
	//printf("%d: leave send\n", ProcessID);
	return r;

}

int MPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{   
	//printf("%d: enter Isend\n", ProcessID);
        int r = PMPI_Isend(buf, count, datatype, dest, tag, comm, request);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Isend: dest is %d, tag is %d, type is %d, comm is %d\n",dest, tag, datatype,comm);
	fputs(buf,file);
	}
	return r;
}

int MPI_Issend(void * buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request * request)
{		
	//printf("%d: enter Issend\n", ProcessID);
        int r = PMPI_Issend(buf, count, datatype, dest, tag, comm, request);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Issend: dest is %d, tag is %d, type is %d, comm is %d\n",dest, tag, datatype,comm);
	fputs(buf,file);
	}
		return r;
}

int MPI_Recv(void *buf,int count,MPI_Datatype datatype,int source,int tag,MPI_Comm comm,MPI_Status *status)
{

        int r1;
	//if(IfFileOpen)
	//{
	r1 = PMPI_Recv(buf,count,datatype,source,tag,comm,status);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Recv: source is %d, tag is %d, type is %d, comm is %d\n",source, tag, datatype,comm);
	fputs(buf,file);
	}
	//}
	return r1;

}

int MPI_Irecv(void * buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request * request)
{
        int r1;
	r1 = PMPI_Irecv(buf,count,datatype,source,tag,comm,request);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Recv: source is %d, tag is %d, type is %d, comm is %d\n",source, tag, datatype,comm);
	fputs(buf,file);
	}
	
	return r1;

}

int MPI_Sendrecv(void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Status *status)
{
	int r = PMPI_Sendrecv(sendbuf,sendcount,sendtype,dest,sendtag,recvbuf,recvcount,recvtype,source,recvtag,comm,status);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"SendRecv: comm is %d\n",comm);
	fputs(buf,file);
	}
	return r;
	
	
}

int MPI_Wait(MPI_Request *request, MPI_Status *status)
{
	
        //printf("%d: enter wait\n", ProcessID);
	int r = PMPI_Wait(request,status);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Wait:\n");
	fputs(buf,file);
	}

	return r;
}

int MPI_Waitall(int count, MPI_Request *array_of_requests, MPI_Status *array_of_statuses)
{
	int r = PMPI_Waitall(count, array_of_requests,array_of_statuses);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Waitall:\n");
	fputs(buf,file);
	}

	return r;
    
}

int MPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
	int r= PMPI_Alltoall(sendbuf, sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Alltoall: comm is %d\n", comm);
	fputs(buf,file);
	}
 	return r;
 	       
}

int MPI_Alltoallv(void* sendbuf, int *sendcounts, int *sdispls, MPI_Datatype sendtype, void* recvbuf, int *recvcounts, int *rdispls, MPI_Datatype recvtype, MPI_Comm comm)
{
	int r= PMPI_Alltoallv(sendbuf, sendcounts,sdispls,sendtype,recvbuf,recvcounts,rdispls,recvtype,comm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Alltoallv: comm is %d\n", comm);
	fputs(buf,file);
	}
 	return r;
}

int MPI_Allreduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{	
	int r = PMPI_Allreduce(sendbuf,recvbuf,count,datatype,op,comm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Allreduce: comm is %d\n", comm);
	fputs(buf,file);
	}
	return r;	
}

int MPI_Reduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm)
{
	int r = PMPI_Reduce(sendbuf,recvbuf,count,datatype,op,root,comm);

	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Reduce: comm is %d\n", comm);
	fputs(buf,file);
	}
	return r;
}

int MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
        int r = PMPI_Bcast(buffer, count, datatype, root, comm);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Bcast: comm is %d\n", comm);
	fputs(buf,file);
	}
	return r;
        
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status)
{
        //printf("%d, enter Iprobe\n", ProcessID);
        int r = PMPI_Iprobe(source,tag,comm,flag,status);
	if(ProcessID == printID)
	{
	char buf[200];
	sprintf(buf,"Iprobe: comm is %d\n", comm);
	fputs(buf,file);
	}
	return r;
	
		
}

int MPI_Finalize()
{
		fclose(file);
		freeMyHashMap(table);
		return PMPI_Finalize();
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




