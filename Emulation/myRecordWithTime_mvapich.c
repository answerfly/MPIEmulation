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
int comm_size;
double starttime;
double stoptime;
int ifrecord = 0;
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
int MPI_Comm_size(MPI_Comm comm, int *size)
{
		//printf("%d: enter comm_size\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Comm_size(comm,size);
			return r;
		}
	    int r = PMPI_Comm_size(comm,size);
		fwrite(size,sizeof(int),1,commfile);
		//printf("%d: leave comm_size,size is %d\n", ProcessID,*size);
		return r;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{
		//printf("%d: enter comm_rank\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Comm_rank(comm,rank);
			return r;
		}
	    int r = PMPI_Comm_rank(comm,rank);
		fwrite(rank,sizeof(int),1,commfile);
		//printf("%d: leave comm_rank, rank is %d\n", ProcessID, *rank);
		return r;

}

int MPI_Comm_split(MPI_Comm comm,int color, int key,MPI_Comm *newcomm)
{
		//printf("%d: enter comm_split\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Comm_split(comm,color,key,newcomm);
			return r;
		}
	    int r = PMPI_Comm_split(comm,color,key,newcomm);
		fwrite(newcomm,sizeof(int),1,commfile);
		//printf("%d: leave comm_split, newcomm is %d\n", ProcessID, *newcomm);
		return r;
}

int MPI_Comm_group(MPI_Comm comm, MPI_Group * group)
{
		//printf("%d: enter comm_group\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Comm_group(comm,group);
			return r;
		}
	    int r = PMPI_Comm_group(comm,group);
		fwrite(group,sizeof(int),1,commfile);
		//printf("%d: leave comm_group, group is %d\n", ProcessID, *group);
		return r;
}

int MPI_Group_translate_ranks(MPI_Group group1, int n, const int * ranks1, MPI_Group group2, const int * ranks2)
{
		//printf("%d: enter group_translate\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Group_translate_ranks(group1,n,ranks1,group2,ranks2);
			return r;
		}
	    int r = PMPI_Group_translate_ranks(group1,n,ranks1,group2,ranks2);
		fwrite(&n,sizeof(int),1,commfile);
		fwrite(ranks2,sizeof(int),n,commfile);
		//printf("%d: leave group_translate, n is %d\n", ProcessID, n);
		return r;

}

int MPI_Group_incl(MPI_Group group,int n,const int *ranks,MPI_Group *newgroup)
{
		//printf("%d: enter group_incl\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Group_incl(group,n,ranks,newgroup);
			return r;
		}
	    int r = PMPI_Group_incl(group,n,ranks,newgroup);
		fwrite(newgroup,sizeof(int),1,commfile);
		//printf("%d: leave group_incl, newgroup is %d\n", ProcessID, *newgroup);
		return r;
		
}

int MPI_Comm_create(MPI_Comm comm,MPI_Group group,MPI_Comm *newcomm)
{
		//printf("%d: enter comm_create\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Comm_create(comm,group,newcomm);
			return r;
		}
	    int r = PMPI_Comm_create(comm,group,newcomm);
		fwrite(newcomm,sizeof(int),1,commfile);
		//printf("%d: leave comm_create,newcomm is %d\n", ProcessID, *newcomm);
		return r;
}

	//struct timeval tv;
	//gettimeofday(&tv,NULL);
	//printf("second:%ld\n",tv.tv_sec); 
	//printf("millisecond:%ld\n",tv.tv_sec*1000 + tv.tv_usec/1000);
        //printf("microsecond:%ld\n",tv.tv_sec*1000000 + tv.tv_usec);  

int MPI_Init(int *argc,char ***argv)
{
	//printf("enter init\n");
	char filename[10];
	//char commfilename[10];
	char ProcessID_String[10];
	int r = PMPI_Init(argc,argv);
	//printf("program is %s\n", *(*argv+0) );
	PMPI_Comm_rank(MPI_COMM_WORLD,&ProcessID);
	if ( (ProcessID % 8) == 0 )
	{
		ifrecord = 1;
	}
	else
	{
		//printf("leave init\n");
		return r;
	}
	//printf("processID is %d\n",ProcessID);
	memcpy(filename, *(*argv) + 2, 2);
	memcpy(filename + 2,"log", 3);
    int len = int2char(ProcessID,ProcessID_String);
	//printf("len is %d\n",len);
    memcpy(filename+5,ProcessID_String,len);
    filename[5+len]='\0';
        //printf("log name is %s\n", filename);	
    if((file=fopen(filename,"w"))==NULL) 
	{
		return 1;
	}
	memcpy(filename, *(*argv) + 2, 2);
	memcpy(filename + 2,"comm", 4);
    memcpy(filename+6,ProcessID_String,len);
    filename[6+len]='\0';
        //printf("log name is %s\n", filename);	
    if((commfile=fopen(filename,"w"))==NULL) 
	{
		return 1;
	}
	table = createMyHashMap(myHashCodeRequest,myEqualRequest);
	//printf("%d: leave init\n", ProcessID);

	return r;
}

int MPI_Send(const void *buf,int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
	//printf("%d: enter send\n", ProcessID);
	if (ifrecord ==0 )
	{
		int r = PMPI_Send(buf, count,datatype,dest,tag,comm);
		return r;
	}
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Send(buf, count,datatype,dest,tag,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
    int type_size;
    MPI_Type_size(datatype, &type_size);
	RMR rmr;
	rmr.MsgType = Send;
	rmr.typesize = type_size;
	rmr.sendlength = type_size * count;
	rmr.datatype = datatype;
	rmr.source = dest;
	rmr.tag = tag;
	rmr.comm = comm;
	rmr.recvcount = 0;
	rmr.typesize = 0;
	//rmr.status = NULL;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
    rmr.returnvalue = r;
	fwrite(&rmr,sizeof(RMR),1,file);		
	//printf("%d: leave send\n", ProcessID);
	return r;

}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{   
		//printf("%d: enter Isend\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Isend(buf, count, datatype, dest, tag, comm, request);
			return r;
		}
        int r = PMPI_Isend(buf, count, datatype, dest, tag, comm, request);
        //struct timeval stop;
        //gettimeofday(&stop,NULL);
        int type_size;
        MPI_Type_size(datatype, &type_size);
        TMR *tmr = (TMR *)malloc(sizeof(TMR));
        tmr->MsgType = Isend;
        //tmr->typesize = getsize((MPI_Datatype)(*datatype));
        tmr->typesize = type_size;
        tmr->datatype = datatype;
        tmr->source = dest;
        tmr->tag = tag;
        tmr->comm = comm;
        tmr->returnvalue = 0;
        tmr->buf = NULL;
		tmr->count = count;
        //tmr.time = start.tv_sec*1000000 + start.tv_usec - stop.tv_sec * 1000000 - stop.tv_usec;
        MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
        *request_temp = *request;
        myHashMapPutData(table,request_temp,tmr);
		//printf("%d: leave Isend\n", ProcessID);
		return r;
}

int MPI_Issend(const void * buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request * request)
{		
		//printf("%d: enter Issend\n", ProcessID);
		if (ifrecord ==0 )
		{
			int r = PMPI_Issend(buf, count, datatype, dest, tag, comm, request);
			return r;
		}
        //struct timeval start;
        //gettimeofday(&start,NULL);
        int r = PMPI_Issend(buf, count, datatype, dest, tag, comm, request);
        //struct timeval stop;
        //gettimeofday(&stop,NULL);
		int type_size;
		MPI_Type_size(datatype, &type_size);
        TMR *tmr = (TMR *)malloc(sizeof(TMR));
        tmr->MsgType = Issend;
        //tmr->typesize = getsize((MPI_Datatype)(*datatype));
        tmr->typesize = type_size;
        tmr->datatype = datatype;
        tmr->source = dest;
        tmr->tag = tag;
        tmr->comm = comm;
        tmr->returnvalue = 0;
        tmr->buf = NULL;
		tmr->count = count;
            //tmr.time = start.tv_sec*1000000 + start.tv_usec - stop.tv_sec * 1000000 - stop.tv_usec;
        MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
        *request_temp = *request;
        myHashMapPutData(table,request_temp,tmr);
		//printf("%d: leave Issend\n", ProcessID);
		return r;
}

int MPI_Recv(void *buf,int count,MPI_Datatype datatype,int source,int tag,MPI_Comm comm,MPI_Status *status)
{
		//printf("%d: enter recv\n", ProcessID);

	if (ifrecord ==0 )
	{
		int r1 = PMPI_Recv(buf,count,datatype,source,tag,comm,status);
		return r1;;
	}
    int r1;
	//if(IfFileOpen)
	//{
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	r1 = PMPI_Recv(buf,count,datatype,source,tag,comm,status);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int real_count;
	int r2 = PMPI_Get_count(status,datatype,&real_count);
	int type_size;
	MPI_Type_size(datatype, &type_size);
	RMR rmr;
	rmr.MsgType = Recv;
	rmr.recvcount = real_count;
	rmr.datatype = datatype;
	rmr.typesize = type_size;
	rmr.source = source;
	rmr.tag = tag;
	rmr.comm = comm;
	rmr.status = *status;
	rmr.sendlength = 0;
	
	//printf("%d: Recv count is %d\n", ProcessID,real_count);
	rmr.returnvalue = r1;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;

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
	//printf("%d: enter recv\n", ProcessID);
	return r1;

}

int MPI_Irecv(void * buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request * request)
{
	if (ifrecord ==0 )
	{
		int r1 = PMPI_Irecv(buf,count,datatype,source,tag,comm,request);
		return r1;
	}
    int r1;
	//if(IfFileOpen)
	//{
	//struct timeval start;
	//gettimeofday(&start,NULL);
	//printf("%d: enter Irecv\n", ProcessID);
	r1 = PMPI_Irecv(buf,count,datatype,source,tag,comm,request);
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int type_size;
	MPI_Type_size(datatype, &type_size);
	TMR *tmr = (TMR *)malloc(sizeof(TMR));
	tmr->MsgType = Irecv;
	tmr->typesize = type_size;
	tmr->datatype = datatype;
	tmr->source = source;
	tmr->tag = tag;
	tmr->comm = comm;
	tmr->returnvalue = 0;
	tmr->buf = buf;
	tmr->count = count;
        //tmr.time = start.tv_sec*1000000 + start.tv_usec - stop.tv_sec * 1000000 - stop.tv_usec;
	MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_temp = *request;
	myHashMapPutData(table,request_temp,tmr);
	//printf("%d: leave Irecv, table number is %d\n", ProcessID, myHashMapGetSize(table) );
	//printf("%d: leave Irecv, request is %d\n", ProcessID,*request_temp);
	//}
	
	return r1;

}

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Status *status)
{
	//printf("%d: enter sendrecv\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	if (ifrecord ==0 )
	{
		int r = PMPI_Sendrecv(sendbuf,sendcount,sendtype,dest,sendtag,recvbuf,recvcount,recvtype,source,recvtag,comm,status);
		return r;
	}
	starttime = MPI_Wtime();
	int r = PMPI_Sendrecv(sendbuf,sendcount,sendtype,dest,sendtag,recvbuf,recvcount,recvtype,source,recvtag,comm,status);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int send_type_size;
	MPI_Type_size(sendtype, &send_type_size);
	int recv_type_size;
	MPI_Type_size(recvtype, &recv_type_size);
	int real_count;
	int r2 = PMPI_Get_count(status,recvtype,&real_count);
	RMR rmr;
	rmr.MsgType = Sendrecv;
	rmr.recvcount = real_count;
	rmr.datatype = recvtype;
	rmr.typesize = recv_type_size;
	rmr.source = source;
	rmr.tag = recvtag;
	rmr.comm = comm;
	rmr.status = *status;
	rmr.sendlength = sendcount * send_type_size;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
    fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,real_count,file);
	//printf("%d: leave sendrecv\n", ProcessID);
	return r;
	
	
}

int MPI_Wait(MPI_Request *request, MPI_Status *status)
{
	
    //printf("%d: enter wait\n", ProcessID);
	if (ifrecord ==0 )
	{
		int r = PMPI_Wait(request,status);
		return r;
	}
	MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_temp = *request;
	
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Wait(request,status);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	TMR * tmr = myHashMapGetDataByKey(table,request_temp);

    //printf("%d: in wait\n", ProcessID);
	MPI_Status resultstauts = *status;
	int real_count;
	int r2 = PMPI_Get_count(status,tmr->datatype,&real_count);
	//printf("Wait: length is %d, count is %d\n", real_count,resultstauts.count);
    RMR rmr;
	rmr.MsgType = tmr->MsgType;
	rmr.recvcount = real_count;
	//rmr.recvcount = resultstauts.count;
	rmr.typesize = tmr->typesize;
	rmr.source = tmr->source;
	rmr.tag = tmr->tag;
	rmr.comm = tmr->comm;
	rmr.status = resultstauts;
	rmr.datatype = tmr->datatype;
	//rmr.returnvalue = tmr->returnvalue;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;

    if(tmr->MsgType == Isend || tmr->MsgType == Issend)
    {   
                        //printf("%d: waitall: Isend\n", ProcessID);
		rmr.recvcount = 0;
        rmr.sendlength = real_count * tmr->typesize;;
        fwrite(&rmr,sizeof(RMR),1,file);
    }   
    else
    {   
                        //printf("%d: waitall: Irecv, count is %d\n", ProcessID, real_count);
        rmr.recvcount = real_count;
        rmr.sendlength = 0;
        fwrite(&rmr,sizeof(RMR),1,file);
        fwrite(tmr->buf,rmr.typesize,rmr.recvcount,file);
	}
   

    myHashMapRemoveDataByKey(table,request_temp);
	free(request_temp);
	//printf("%d: leave wait, table number is %d\n", ProcessID, myHashMapGetSize(table) );
	//printf("%d: leave wait\n", ProcessID);

	return r;
}

int MPI_Waitall(int count, MPI_Request *array_of_requests, MPI_Status *array_of_statuses)
{
		if (ifrecord ==0 )
		{
			int r = PMPI_Waitall(count,array_of_requests,array_of_statuses);
		}
    
        //printf("%d: enter waitall,count is %d\n", ProcessID,count);
		if(array_of_statuses != MPI_STATUS_IGNORE)
		{
        for(int i=0; i<count; i++)
        {   
                    
                MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
                *request_temp = *(array_of_requests + i);
				//printf("%d: waitall,request is %d\n", ProcessID,*request_temp);
                
                //struct timeval start;
                //gettimeofday(&start,NULL);
				starttime = MPI_Wtime();
                int r = PMPI_Wait(array_of_requests + i,array_of_statuses +i);
				stoptime = MPI_Wtime();
                //struct timeval stop;
                //gettimeofday(&stop,NULL);
                TMR * tmr = myHashMapGetDataByKey(table,request_temp);
                /*if (tmr != NULL) 
				{
					printf("%d:Waitall: tmr is not null\n",ProcessID);
					printf("type is %d, datatype is %d, source is %d, tag is %d\n", tmr->MsgType,tmr->datatype,tmr->source,tmr->tag);
				}*/
                MPI_Status resultstatus = *(array_of_statuses + i);
                int real_count;
                int r2 = PMPI_Get_count(array_of_statuses + i,tmr->datatype, &real_count);
                RMR rmr;
                rmr.MsgType = tmr->MsgType;
                rmr.typesize = tmr->typesize;
                rmr.source = tmr->source;
                rmr.tag = tmr->tag;
                rmr.comm = tmr->comm;
                rmr.status = resultstatus;
                rmr.datatype = tmr->datatype;
                rmr.returnvalue = r;
                //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
				rmr.time = stoptime -starttime;
                if(tmr->MsgType == Isend || tmr->MsgType == Issend)
                {   
                        //printf("%d: waitall: Isend\n", ProcessID);
                        rmr.recvcount = 0;
                        rmr.sendlength = real_count * tmr->typesize;;
                        fwrite(&rmr,sizeof(RMR),1,file);
                }   
                else
                {   
                        //printf("%d: waitall: Irecv, count is %d\n", ProcessID, real_count);
                        rmr.recvcount = real_count;
                        rmr.sendlength = 0;
                        fwrite(&rmr,sizeof(RMR),1,file);
                        fwrite(tmr->buf,rmr.typesize,rmr.recvcount,file);
				}
   
   
                myHashMapRemoveDataByKey(table,request_temp);
        }
		}
		else
		{
        for(int i=0; i<count; i++)
        {   
                    
                MPI_Request *request_temp = (MPI_Request *)malloc(sizeof(MPI_Request));
                *request_temp = *(array_of_requests + i);
				//printf("%d: waitall,request is %d\n", ProcessID,*request_temp);
                
                //struct timeval start;
                //gettimeofday(&start,NULL);
				starttime = MPI_Wtime();
                int r = PMPI_Wait(array_of_requests + i,MPI_STATUS_IGNORE);
				stoptime = MPI_Wtime();
                //struct timeval stop;
                //gettimeofday(&stop,NULL);
                TMR * tmr = myHashMapGetDataByKey(table,request_temp);
                //MPI_Status resultstatus = *(array_of_status + i);
                MPI_Status resultstatus;
                RMR rmr;
                rmr.MsgType = tmr->MsgType;
                rmr.typesize = tmr->typesize;
                rmr.source = tmr->source;
                rmr.tag = tmr->tag;
                rmr.comm = tmr->comm;
                rmr.status = resultstatus;
                rmr.datatype = tmr->datatype;
                rmr.returnvalue = r;
                //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
				rmr.time = stoptime -starttime;
                if(tmr->MsgType == Isend || tmr->MsgType == Issend)
                {   
                        //printf("%d: waitall: Isend\n", ProcessID);
                        rmr.recvcount = 0;
                        rmr.sendlength = tmr->count * tmr->typesize;;
                        fwrite(&rmr,sizeof(RMR),1,file);
                }   
                else
                {   
                        //printf("%d: waitall: Irecv\n", ProcessID);
                        rmr.recvcount = tmr->count;
                        rmr.sendlength = 0;
                        fwrite(&rmr,sizeof(RMR),1,file);
                        fwrite(tmr->buf,rmr.typesize,rmr.recvcount,file);
				}
   
   
                myHashMapRemoveDataByKey(table,request_temp);
        }

		}
        //printf("%d: leave waitall\n", ProcessID);
}

int MPI_Alltoall(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
	if (ifrecord ==0 )
	{
		int r= PMPI_Alltoall(sendbuf, sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
		return r;
	}
	//printf("%d: enter alltoall\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r= PMPI_Alltoall(sendbuf, sendcount,sendtype,recvbuf,recvcount,recvtype,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int procNum;
	PMPI_Comm_size(comm,&procNum);
	int send_type_size;
	MPI_Type_size(sendtype, &send_type_size);
	int recv_type_size;
	MPI_Type_size(recvtype, &recv_type_size);
	RMR rmr;
	rmr.MsgType = Alltoall;
	rmr.datatype = recvtype;
	rmr.typesize = recv_type_size;
        rmr.recvcount = recvcount * procNum;
	//mr.sendtype = sendtype;
	//mr.sendcount = sendcount*procNum;
	rmr.comm = comm;
	rmr.sendlength = send_type_size * sendcount * procNum;
	rmr.returnvalue = r;
        //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,recvcount * procNum,file);
	//printf("%d: leave alltoall\n", ProcessID);
 	return r;
 	       
}

int MPI_Alltoallv(const void* sendbuf, int *sendcounts, int *sdispls, MPI_Datatype sendtype, void* recvbuf, int *recvcounts, int *rdispls, MPI_Datatype recvtype, MPI_Comm comm)
{
	if (ifrecord ==0 )
	{
		int r= PMPI_Alltoallv(sendbuf, sendcounts,sdispls,sendtype,recvbuf,recvcounts,rdispls,recvtype,comm);
		return r;
	}
	//printf("%d: leave alltoallv\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r= PMPI_Alltoallv(sendbuf, sendcounts,sdispls,sendtype,recvbuf,recvcounts,rdispls,recvtype,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int procNum;
	PMPI_Comm_size(comm,&procNum);
	int send_type_size;
	MPI_Type_size(sendtype, &send_type_size);
	int recv_type_size;
	MPI_Type_size(recvtype, &recv_type_size);
	RMR rmr;
	rmr.MsgType = Alltoallv;
	rmr.datatype = recvtype;
	int recvcount = 0;
	int sendcount = 0;
	for (int i=0;i<procNum;i++)
	{
		recvcount = recvcount + recvcounts[i];
		sendcount = sendcount + sendcounts[i];
	}
	rmr.typesize = recv_type_size;
        rmr.recvcount = recvcount;
	rmr.comm = comm;
	rmr.sendlength = send_type_size * sendcount;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,recvcount,file);
	//printf("Alltoallv: sendlength is %d, recvlength is %d\n", rmr.sendlength,rmr.recvcount*rmr.typesize);
	//printf("%d: leave alltoallv\n", ProcessID);
 	return r;
}

int MPI_Allreduce(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{	
	if (ifrecord ==0 )
	{
		int r = PMPI_Allreduce(sendbuf,recvbuf,count,datatype,op,comm);
		return r;
	}
	//printf("%d: enter allreduce\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Allreduce(sendbuf,recvbuf,count,datatype,op,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int type_size;
	MPI_Type_size(datatype, &type_size);
	RMR rmr;
	rmr.MsgType = Allreduce;
	rmr.recvcount = count;
	rmr.comm = comm;
	rmr.datatype = datatype;
	rmr.typesize = type_size;
	rmr.returnvalue = r;
	rmr.sendlength = type_size * count;
        //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;

	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,count,file);
	//printf("%d: leave allreduce\n", ProcessID);
	return r;	
}

int MPI_Reduce(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm)
{
	if (ifrecord ==0 )
	{
		int r = PMPI_Reduce(sendbuf,recvbuf,count,datatype,op,root,comm);
		return r;
	}
	//printf("%d: enter reduce\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Reduce(sendbuf,recvbuf,count,datatype,op,root,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int type_size;
	MPI_Type_size(datatype, &type_size);
	int rank;	
	PMPI_Comm_rank(comm,&rank);
	RMR rmr;
	rmr.recvcount = count;
	rmr.comm = comm;
	rmr.typesize = type_size;
	rmr.datatype = datatype;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
	if(root == rank)
	{
		rmr.MsgType = Reduce;
		rmr.sendlength = 0;
		fwrite(&rmr,sizeof(RMR),1,file);
		fwrite(recvbuf,rmr.typesize,count,file);
	}
	else
	{
		rmr.MsgType = Reduce_NR;
		rmr.sendlength = type_size * count;
		rmr.recvcount = 0;
		fwrite(&rmr,sizeof(RMR),1,file);
	}

	//printf("%d: leave reduce\n", ProcessID);
	return r;
}

int MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
		if (ifrecord ==0 )
		{
			int r = PMPI_Bcast(buffer, count, datatype, root, comm);
			return r;
		}
        //printf("%d, enter bcast, datatype is %d\n", ProcessID,*datatype);
		//printf("%d: enter bcast\n", ProcessID);
        //struct timeval start;
        //gettimeofday(&start,NULL);
		starttime = MPI_Wtime();
        int r = PMPI_Bcast(buffer, count, datatype, root, comm);
		stoptime = MPI_Wtime();
        //struct timeval stop;
        //gettimeofday(&stop,NULL);
        int type_size;
        MPI_Type_size(datatype, &type_size);
        RMR rmr;
        rmr.MsgType = Bcast;
        rmr.recvcount = count;
        rmr.comm = comm;
        rmr.typesize = type_size;
        rmr.sendlength = type_size * count;
        rmr.tag = 50000;
        rmr.source = 50000;
        rmr.datatype = datatype;
        rmr.returnvalue = r;
        //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
		rmr.time = stoptime -starttime;
        
        fwrite(&rmr,sizeof(RMR),1,file);
        fwrite(buffer,rmr.typesize, count, file);
		//printf("%d: leave bcast\n", ProcessID);
		return r;
        
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status)
{
		if (ifrecord ==0 )
		{
			int r = PMPI_Iprobe(source,tag,comm,flag,status);
			return r;
		}
        //printf("%d, enter Iprobe\n", ProcessID);
        //struct timeval start;
        //gettimeofday(&start,NULL);
        int r = PMPI_Iprobe(source,tag,comm,flag,status);
        //struct timeval stop;
        //gettimeofday(&stop,NULL);
        RMR rmr;
        rmr.MsgType = Iprobe;
        rmr.recvcount = *flag;
        rmr.comm = comm;
        rmr.sendlength = 0;
        rmr.tag = tag;
		rmr.datatype = MPI_INT;
        rmr.source = source;
        rmr.returnvalue = r;
        //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
        fwrite(&rmr,sizeof(RMR),1,file);
        //printf("%d, leave Iprobe\n", ProcessID);
		return r;
		
}

int MPI_Gather(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype,int root, MPI_Comm comm)
{
	if (ifrecord ==0 )
	{
		int r = PMPI_Gather(sendbuf,sendcount, sendtype, recvbuf, recvcount, recvtype,root,comm);
		return r;
	}
    //printf("%d, enter gather\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Gather(sendbuf,sendcount, sendtype, recvbuf, recvcount, recvtype,root,comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int rank;	
	PMPI_Comm_rank(comm,&rank);
	int procNum;
	PMPI_Comm_size(comm,&procNum);
	RMR rmr;
	rmr.comm = comm;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
	int type_size;
	if(root == rank)
	{
		MPI_Type_size(recvtype, &type_size);
		rmr.typesize = type_size;
		rmr.MsgType = Gather;
		rmr.recvcount = recvcount * procNum;
		rmr.sendlength = 0;
		fwrite(&rmr,sizeof(RMR),1,file);
		fwrite(recvbuf,rmr.typesize,rmr.recvcount,file);
	}
	else
	{
		MPI_Type_size(sendtype, &type_size);
		rmr.MsgType = Gather_NR;
		rmr.sendlength = type_size * sendcount;
		rmr.recvcount = 0;
		fwrite(&rmr,sizeof(RMR),1,file);
	}
    //printf("%d, leave gather\n", ProcessID);

	return r;
		
}

int MPI_Allgather(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
	if (ifrecord ==0 )
	{
		int r = PMPI_Allgather(sendbuf,sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
		return r;
	}
    //printf("%d, enter allgather\n", ProcessID);
	//struct timeval start;
	//gettimeofday(&start,NULL);
	starttime = MPI_Wtime();
	int r = PMPI_Allgather(sendbuf,sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
	stoptime = MPI_Wtime();
	//struct timeval stop;
	//gettimeofday(&stop,NULL);
	int procNum;
	PMPI_Comm_size(comm,&procNum);
	RMR rmr;
	rmr.comm = comm;
	rmr.returnvalue = r;
    //rmr.time = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
    rmr.time = stoptime -starttime;
	int recv_type_size;
	MPI_Type_size(recvtype, &recv_type_size);
	int send_type_size;
	MPI_Type_size(sendtype, &send_type_size);
	rmr.typesize = recv_type_size;
	rmr.MsgType = Allgather;
	rmr.recvcount = recvcount * procNum;
	rmr.sendlength = send_type_size * sendcount;
	fwrite(&rmr,sizeof(RMR),1,file);
	fwrite(recvbuf,rmr.typesize,rmr.recvcount,file);
    //printf("%d, leave allgather\n", ProcessID);

	return r;
}

/*int MPI_Barrier(MPI_Comm comm)
{
        struct timeval start;
        gettimeofday(&start,NULL);
		int r = PMPI_Barrier(comm);
        struct timeval stop;
        gettimeofday(&stop,NULL);
		return r;
}*/


int MPI_Finalize()
{
		if (ifrecord ==0 )
		{
			return PMPI_Finalize();
		}
		fclose(file);
		fclose(commfile);
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




