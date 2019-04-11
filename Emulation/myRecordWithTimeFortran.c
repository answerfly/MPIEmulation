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
static FILE *commfile;
MyHashMap *table;
int ProcessID;
int ifrecord = 0;
double starttime;
double stoptime;
//int int2char(int id,char *idString);
//int char2int(char *ProcessID_String);
/*int MPI_Comm_size(MPI_Comm comm, int *size)
{
	*size = 4;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{
	int id;
	PMPI_Comm_rank(MPI_COMM_WORLD,&id);
	if(id == 0) *rank = 0;
	else *rank = 3;
	return 1;
	
}*/
void mpi_comm_size_ ( int *comm, int *size, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Comm_size( (MPI_Comm)(*comm), size );
		return;
	}
	*ierr = MPI_Comm_size( (MPI_Comm)(*comm), size );
    fwrite(size,sizeof(int),1,commfile);
}

void mpi_comm_rank_ ( int *comm, int *rank, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Comm_rank( (MPI_Comm)(*comm), rank );
		return;
	}
	*ierr = MPI_Comm_rank( (MPI_Comm)(*comm), rank );
    fwrite(rank,sizeof(int),1,commfile);
}

void mpi_comm_split_ ( int *comm, int *color, int *key, int *newcomm, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Comm_split( (MPI_Comm)(*comm), *color, *key, (MPI_Comm *)(newcomm) );
		return;
	}
	*ierr = MPI_Comm_split( (MPI_Comm)(*comm), *color, *key, (MPI_Comm *)(newcomm) );
    fwrite(newcomm,sizeof(int),1,commfile);
}

void mpi_comm_group_ ( int *comm, int *group, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Comm_group( (MPI_Comm)(*comm), group );
		return;
	}
	*ierr = MPI_Comm_group( (MPI_Comm)(*comm), group );
    fwrite(group,sizeof(int),1,commfile);
}

void mpi_group_translate_ranks_ ( int *group1, int *n, int *ranks1, int *group2, int *ranks2, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Group_translate_ranks( *group1, *n, ranks1, *group2, ranks2 );
		return;
	}
	*ierr = MPI_Group_translate_ranks( *group1, *n, ranks1, *group2, ranks2 );
    fwrite(n,sizeof(int),1,commfile);
    fwrite(ranks2,sizeof(int),*n,commfile);

}

void mpi_group_incl_ ( int *group, int *n, int *ranks, int *newgroup, int *ierr )
{
	if(ifrecord == 0)
	{
	    *ierr = MPI_Group_incl( *group, *n, ranks, newgroup );
		return;
	}
	*ierr = MPI_Group_incl( *group, *n, ranks, newgroup );
    fwrite(newgroup,sizeof(int),1,commfile);
}

void mpi_comm_create_ ( int *comm, int *group, int *newcomm, int *ierr )
{
	if(ifrecord == 0)
	{
		*ierr = MPI_Comm_create( (MPI_Comm)(*comm), *group, (MPI_Comm *)(newcomm) );
		return;
	}
	*ierr = MPI_Comm_create( (MPI_Comm)(*comm), *group, (MPI_Comm *)(newcomm) );
    fwrite(newcomm,sizeof(int),1,commfile);
	
}

void mpi_comm_dup_ ( int *comm, int *newcomm, int *ierr)
{
	if(ifrecord == 0)
	{
		*ierr = MPI_Comm_dup( (MPI_Comm)(*comm), (MPI_Comm *)(newcomm) );
		return;
	}
	*ierr = MPI_Comm_dup( (MPI_Comm)(*v1), (MPI_Comm *)(v2) );
    	fwrite(newcomm,sizeof(int),1,commfile);
}


void mpi_init_(int *ierr)
{
	char filename[10];
	char ProcessID_String[10];
	*ierr = MPI_Init(0,0);
	MPI_Comm_rank(MPI_COMM_WORLD,&ProcessID);
	if ((ProcessID % 8) == 0)
	{
		ifrecord = 1;
	}
	else
		return;
	//printf("processID is %d\n",ProcessID);
	//strcpy(filename,"log");
	memcpy(filename,"log",3);
    int len = int2char(ProcessID,ProcessID_String);
	//printf("len is %d\n",len);
    memcpy(filename+3,ProcessID_String,len);
    filename[3+len]='\0';
        //printf("log name is %s\n", filename);	
    if((file=fopen(filename,"w"))==NULL) 
	{
		return;
	}
	memcpy(filename,"comm",4);
    memcpy(filename+4,ProcessID_String,len);
    filename[4+len]='\0';
    if((commfile=fopen(filename,"w"))==NULL) 
	{
		return;
	}
	table = createMyHashMap(myHashCodeRequest,myEqualRequest);
	printf("leave init is \n");

}

/*void mpi_comm_dup_(int *comm, int *newcomm, int *ierr)
{
	*ierr = MPI_Comm_dup( (MPI_Comm)(*comm), (MPI_Comm *)(newcomm) );
	//printf("source comm is %d, new comm is %d\n", *comm, *newcomm);
}

void mpi_comm_split_(int *comm, int *color, int *key, int *newcomm, int *ierr)
{
	*ierr = MPI_Comm_split((MPI_Comm)(*comm), *color, *key, (MPI_Comm *)(newcomm));
	//printf("source comm is %d, new comm is %d. color is %d, key is %d\n", *comm, *newcomm, *color, *key);
}*/

void mpi_send_(void *buf,int *count, int *datatype, int *dest, int *tag, int *comm, int *ierr)
{
	//printf("%d: enter send\n", ProcessID);
	if (ifrecord == 0)
	{
		*ierr = MPI_Send(buf, *count, (MPI_Datatype)(*datatype),*dest,*tag,(MPI_Comm)(*comm));
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Send(buf, *count, (MPI_Datatype)(*datatype),*dest,*tag,(MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	RMR rmr;
	rmr.MsgType = Send;
	//rmr.typesize = getsize((MPI_Datatype)(*datatype));
	//rmr.sendlength = getsize((MPI_Datatype)(*datatype)) * (*count);
	rmr.typesize = type_size;
	rmr.sendlength = type_size * (*count);
	rmr.datatype = (MPI_Datatype)(*datatype);
	rmr.source = *dest;
	rmr.tag = *tag;
	rmr.comm = (MPI_Comm)(*comm);
    rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;
	fwrite(&rmr,sizeof(RMR),1,file);		
	//printf("%d: leave send\n", ProcessID);

}

void mpi_isend_(void *buf, int *count, int *datatype, int *dest, int *tag, int *comm, int *request, int *ierr)
{	
	if (ifrecord == 0)
	{
		*ierr = MPI_Isend(buf, *count, (MPI_Datatype)(*datatype), *dest, *tag, (MPI_Comm)(*comm), (MPI_Request *)request);
		return;
	}
	*ierr = MPI_Isend(buf, *count, (MPI_Datatype)(*datatype), *dest, *tag, (MPI_Comm)(*comm), (MPI_Request *)request);
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	TMR *tmr = (TMR *)malloc(sizeof(TMR));
	tmr->MsgType = Isend;
	//tmr->typesize = getsize((MPI_Datatype)(*datatype));
	tmr->typesize = type_size;
	tmr->datatype = (MPI_Datatype)(*datatype);
	tmr->source = *dest;
	tmr->tag = *tag;
	tmr->comm = (MPI_Comm)(*comm);
	tmr->returnvalue = 0;
	tmr->buf = NULL;
        //tmr.time = start.tv_sec*1000000 + start.tv_usec - stop.tv_sec * 1000000 - stop.tv_usec;
	MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_temp = *((MPI_Request *)request);
	myHashMapPutData(table,request_temp,tmr);
} 

void mpi_recv_(void *buf,int *count,int *datatype,int *source,int *tag,int *comm,int *status, int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Recv(buf,*count,(MPI_Datatype)(*datatype),*source,*tag,(MPI_Comm)(*comm),(MPI_Status *)status);
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Recv(buf,*count,(MPI_Datatype)(*datatype),*source,*tag,(MPI_Comm)(*comm),(MPI_Status *)status);
	stoptime = MPI_Wtime();
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int real_count;
	int r2 = MPI_Get_count((MPI_Status *)status,(MPI_Datatype)(*datatype),&real_count);
	RMR rmr;
	rmr.MsgType = Recv;
	rmr.recvcount = real_count;
	rmr.datatype = (MPI_Datatype)(*datatype);
	//rmr.typesize = getsize((MPI_Datatype)(*datatype));
	rmr.typesize = type_size;
	rmr.source = *source;
	rmr.tag = *tag;
	rmr.comm = (MPI_Comm)(*comm);
	rmr.status = *((MPI_Status *)status);
	rmr.sendlength = 0;
	rmr.time = stoptime - starttime;
	
	//printf("%d: Recv length is %d, count is %d\n", ProcessID,length,rmr.status.count);
	rmr.returnvalue = *ierr;

	/*if(length == 1)
	{
		char *value = (char *)buf;	
		int ifnull = 0;	
		if(*value == '\0') ifnull = 1;
		printf("Recv: ifnull is %d\n", ifnull);
	}*/
    fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(buf,rmr.typesize,real_count,file);
	//}

}

void mpi_irecv_(void * buf, int *count, int *datatype, int *source, int *tag, int *comm, int *request, int *ierr)
{
	//if(IfFileOpen)
	//printf("%d: enter irecv\n", ProcessID);
	//{
	//struct timeval start;
	//gettimeofday(&start,NULL);
	if (ifrecord == 0)
	{
		*ierr = MPI_Irecv(buf,*count,(MPI_Datatype)(*datatype),*source,*tag,(MPI_Comm)(*comm),(MPI_Request *)(request));
		return;
	}
	*ierr = MPI_Irecv(buf,*count,(MPI_Datatype)(*datatype),*source,*tag,(MPI_Comm)(*comm),(MPI_Request *)(request));
	//printf("%d: irecv, request is %d\n", ProcessID, *request);
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	TMR *tmr = (TMR *)malloc(sizeof(TMR));
	tmr->MsgType = Irecv;
	//tmr->typesize = getsize((MPI_Datatype)(*datatype));
	tmr->typesize = type_size;
	tmr->datatype = (MPI_Datatype)(*datatype);
	tmr->source = *source;
	tmr->tag = *tag;
	tmr->comm = (MPI_Comm)(*comm);
	tmr->returnvalue = 0;
	tmr->buf = buf;
        //tmr.time = start.tv_sec*1000000 + start.tv_usec - stop.tv_sec * 1000000 - stop.tv_usec;
	MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_temp = *((MPI_Request *)request);
	//printf("%d: irecv, request_tmp is %d\n", ProcessID, *request_temp);
	myHashMapPutData(table,request_temp,tmr);
	//}
	

}

void mpi_wait_(int *request, int *status, int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Wait((MPI_Request *)(request),(MPI_Status *)(status));
		return;
	}
	
	//printf("%d: enter wait\n", ProcessID);
	//printf("%d: wait, request is %d\n", ProcessID, *request);
	MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_temp = *((MPI_Request *)request);
	
	//printf("%d: wait, request_temp is %d\n", ProcessID, *request_temp);
	starttime = MPI_Wtime();
	*ierr = MPI_Wait((MPI_Request *)(request),(MPI_Status *)(status));
	stoptime = MPI_Wtime();
	TMR * tmr = myHashMapGetDataByKey(table,request_temp);
	MPI_Status resultstauts = *((MPI_Status *)status);
	int count;
	int r2 = MPI_Get_count((MPI_Status *)(status),tmr->datatype,&count);
	//printf("Wait: length is %d, count is %d\n", count,resultstauts.count);
        RMR rmr;
	rmr.MsgType = tmr->MsgType;
	rmr.recvcount = count;
	//rmr.recvcount = resultstauts.count;
	rmr.typesize = tmr->typesize;
	rmr.source = tmr->source;
	rmr.tag = tmr->tag;
	rmr.comm = tmr->comm;
	rmr.status = resultstauts;
	rmr.datatype = tmr->datatype;
	//rmr.returnvalue = tmr->returnvalue;
	rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;

	if(tmr->MsgType == Isend)
	{
		rmr.recvcount = 0;
		rmr.sendlength = count * tmr->typesize;;
        	fwrite(&rmr,sizeof(RMR),1,file);
	}
	else
	{
		rmr.recvcount = count;
		rmr.sendlength = 0;
        	fwrite(&rmr,sizeof(RMR),1,file);
		fwrite(tmr->buf,rmr.typesize,rmr.recvcount,file);
	}

        myHashMapRemoveDataByKey(table,request_temp);
	//printf("%d: leave wai\n", ProcessID);

}

void mpi_waitall_(int *count, int *array_of_requests, int *array_of_statuses, int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Waitall( *count, (MPI_Request *)(array_of_requests),(MPI_Status *)(array_of_statuses) );
		return;
	}
	
	//printf("%d: enter waitall\n", ProcessID);
	for(int i=0; i<(*count); i++)
	{
		
		MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
		*request_temp = *((MPI_Request *)(array_of_requests + i));
	
		starttime = MPI_Wtime();
		*ierr = MPI_Wait((MPI_Request *)(array_of_requests + i),(MPI_Status *)(array_of_statuses +i));
		stoptime = MPI_Wtime();
		TMR * tmr = myHashMapGetDataByKey(table,request_temp);
		MPI_Status resultstauts = *((MPI_Status *)(array_of_statuses + i));
		int real_count;
		int r2 = MPI_Get_count((MPI_Status *)(array_of_requests + i),tmr->datatype,&real_count);
		//printf("Wait: length is %d, count is %d\n", count,resultstauts.count);
        	RMR rmr;
		rmr.MsgType = tmr->MsgType;
		rmr.typesize = tmr->typesize;
		rmr.source = tmr->source;
		rmr.tag = tmr->tag;
		rmr.comm = tmr->comm;
		rmr.status = resultstauts;
		rmr.datatype = tmr->datatype;
		rmr.returnvalue = *ierr;
		rmr.time = stoptime - starttime;
		if(tmr->MsgType == Isend)
		{
			rmr.recvcount = 0;
			rmr.sendlength = real_count * tmr->typesize;;
        		fwrite(&rmr,sizeof(RMR),1,file);
		}
		else
		{
			rmr.recvcount = real_count;
			rmr.sendlength = 0;
        		fwrite(&rmr,sizeof(RMR),1,file);
			fwrite(tmr->buf,rmr.typesize,rmr.recvcount,file);
		}


        	myHashMapRemoveDataByKey(table,request_temp);
	}
	//printf("%d: leave waitall\n", ProcessID);
}

void mpi_alltoall_ (void* sendbuf, int *sendcount, int *sendtype, void* recvbuf, int *recvcount, int *recvtype, int *comm, int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Alltoall(sendbuf, *sendcount, (MPI_Datatype)(*sendtype),recvbuf,*recvcount,(MPI_Datatype)(*recvtype),(MPI_Comm)(*comm));
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Alltoall(sendbuf, *sendcount, (MPI_Datatype)(*sendtype),recvbuf,*recvcount,(MPI_Datatype)(*recvtype),(MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int send_type_size;
	int recv_type_size;
	MPI_Type_size((MPI_Datatype)(*sendtype), &send_type_size);
	MPI_Type_size((MPI_Datatype)(*recvtype), &recv_type_size);
	int procNum;
	MPI_Comm_size((MPI_Comm)(*comm),&procNum);
	RMR rmr;
	rmr.MsgType = Alltoall;
	rmr.datatype = (MPI_Datatype)(*recvtype);
	//rmr.typesize = getsize((MPI_Datatype)(*recvtype));
	rmr.typesize = recv_type_size;
        rmr.recvcount = *recvcount * procNum;
	rmr.tag = 50000;
	rmr.source = 50000;
	rmr.comm = (MPI_Comm)(*comm);
	rmr.sendlength = send_type_size * (*sendcount) * procNum;
	rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,rmr.recvcount,file);
 	       
}

void  mpi_alltoallv_(void* sendbuf, int *sendcounts, int *sdispls, int *sendtype, void* recvbuf, int *recvcounts, int *rdispls, int *recvtype, int *comm, int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Alltoallv(sendbuf, sendcounts,sdispls,(MPI_Datatype)(*sendtype),recvbuf,recvcounts,rdispls,(MPI_Datatype)(*recvtype),(MPI_Comm)(*comm));
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Alltoallv(sendbuf, sendcounts,sdispls,(MPI_Datatype)(*sendtype),recvbuf,recvcounts,rdispls,(MPI_Datatype)(*recvtype),(MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int send_type_size;
	int recv_type_size;
	MPI_Type_size((MPI_Datatype)(*sendtype), &send_type_size);
	MPI_Type_size((MPI_Datatype)(*recvtype), &recv_type_size);
	int procNum;
	MPI_Comm_size((MPI_Comm)(*comm),&procNum);
	RMR rmr;
	rmr.MsgType = Alltoallv;
	rmr.datatype = (MPI_Datatype)(*recvtype);
	int recvcount = 0;
	int sendcount = 0;
	for (int i=0;i<procNum;i++)
	{
		recvcount = recvcount + recvcounts[i];
		sendcount = sendcount + sendcounts[i];
	}
	//rmr.typesize = getsize((MPI_Datatype)(*recvtype));
    rmr.typesize = recv_type_size;
	rmr.recvcount = recvcount;
	rmr.comm = (MPI_Comm)(*comm);
	//rmr.sendlength = getsize((MPI_Datatype)(*sendtype)) * sendcount;
	rmr.sendlength = send_type_size * sendcount;
	rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,recvcount,file);
	//printf("Alltoallv: sendlength is %d, recvlength is %d\n", rmr.sendlength,rmr.recvcount*rmr.typesize);
}

void mpi_allreduce_(void* sendbuf, void* recvbuf, int *count, int *datatype, int *op, int *comm, int *ierr)
{		
	//printf("%d: enter allreduce, datatype is %d\n", ProcessID,*datatype);
	if (ifrecord == 0)
	{
		*ierr = MPI_Allreduce(sendbuf,recvbuf,*count,(MPI_Datatype)(*datatype),*op,(MPI_Comm)(*comm));
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Allreduce(sendbuf,recvbuf,*count,(MPI_Datatype)(*datatype),*op,(MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	RMR rmr;
	rmr.MsgType = Allreduce;
	rmr.recvcount = *count;
	rmr.comm = (MPI_Comm)(*comm);
	rmr.datatype = (MPI_Datatype)(*datatype);
	//rmr.typesize = getsize((MPI_Datatype)(*datatype));
	rmr.typesize = type_size;
	rmr.returnvalue = *ierr;
	//rmr.sendlength = getsize((MPI_Datatype)(*datatype)) * (*count);
	rmr.sendlength = type_size * (*count);
	rmr.tag = 50000;
	rmr.source = 50000;
	rmr.time = stoptime - starttime;

	//printf("Allreduce: sendlength is %d, recvlength is %d, typesize is %d\n", rmr.sendlength,rmr.recvcount*rmr.typesize, rmr.typesize);
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,*count,file);
}

void mpi_reduce_ ( void* sendbuf, void* recvbuf, int *count, int *datatype, int *op, int *root, int *comm, int *ierr )
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Reduce(sendbuf,recvbuf,*count,(MPI_Datatype)(*datatype),*op,*root,(MPI_Comm)(*comm));
		return;
	}
	//printf("enter reduce, datatype is %d\n", *datatype);
	starttime = MPI_Wtime();
	*ierr = MPI_Reduce(sendbuf,recvbuf,*count,(MPI_Datatype)(*datatype),*op,*root,(MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int rank;	
	MPI_Comm_rank((MPI_Comm)(*comm),&rank);
	RMR rmr;
	rmr.recvcount = *count;
	rmr.comm = (MPI_Comm)(*comm);
	//rmr.typesize = getsize((MPI_Datatype)(*datatype));
	rmr.typesize = type_size;
	rmr.tag = 50000;
	rmr.source = 50000;
	rmr.datatype = (MPI_Datatype)(*datatype);
	rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;
	if(*root == rank)
	{
		rmr.MsgType = Reduce;
		rmr.sendlength = 0;
		fwrite(&rmr,sizeof(RMR),1,file);
		fwrite(recvbuf,rmr.typesize,*count,file);
	}
	else
	{
		rmr.MsgType = Reduce_NR;
		rmr.sendlength = type_size * (*count);
		fwrite(&rmr,sizeof(RMR),1,file);
	}

	//return r;
}

void mpi_bcast_ (void* buffer, int *count, int *datatype, int *root, int *comm, int *ierr )
{
	//printf("%d, enter bcast, datatype is %d\n", ProcessID,*datatype);
	if (ifrecord == 0)
	{
		*ierr = MPI_Bcast(buffer, *count, (MPI_Datatype)(*datatype),*root, (MPI_Comm)(*comm));
		return;
	}
	starttime = MPI_Wtime();
	*ierr = MPI_Bcast(buffer, *count, (MPI_Datatype)(*datatype),*root, (MPI_Comm)(*comm));
	stoptime = MPI_Wtime();
	int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	RMR rmr;
	rmr.MsgType = Bcast;
	rmr.recvcount = *count;
	rmr.comm = (MPI_Comm)(*comm);
	//rmr.typesize = getsize((MPI_Datatype)(*datatype));
	//rmr.sendlength = rmr.typesize * (*count);
	rmr.typesize = type_size;
	rmr.sendlength = type_size * (*count);
	rmr.tag = 50000;
	rmr.source = 50000;
	rmr.datatype = (MPI_Datatype)(*datatype);
	rmr.returnvalue = *ierr;
	rmr.time = stoptime - starttime;
	
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(buffer,rmr.typesize,*count,file);
	
}


void mpi_finalize_(int *ierr)
{
	if (ifrecord == 0)
	{
		*ierr = MPI_Finalize();
		return;
	}
	fclose(file);
	fclose(commfile);
	freeMyHashMap(table);
	*ierr = MPI_Finalize();
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




