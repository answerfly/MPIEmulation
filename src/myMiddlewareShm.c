#include<sys/time.h>
#include<unistd.h>
#include<netdb.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include"mpi.h"
#include"myMessageWithTime.h"
#include"myHashCode.h"
#include"myHashMap.h"
#include"myEqual.h"
#include"myList.h"
#include"shm_common.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)


struct shmmessage
{
	int request;
	int response;
	ShmMReq shmmreq;
	ShmMRes shmmres;
};

const size_t BUFFER_SIZE = 4 * 1024;
const size_t BLOCK_SIZE = 1 * 1024 * 1024 - sizeof(ShmMRes);
const size_t BUFFER_SIZE_SMALL = 1024 * 8;
const size_t BLOCK_SIZE_SMALL = 1024 * 8 -sizeof(ShmMRes);
const size_t MESSAGE_SIZE = sizeof(struct shmmessage);
const int TIMEOUT_IN_MS = 500; /* ms */
int Message_Arrive = 0;



static void die(const char *reason);

int get_comm_num(MPI_Comm comm, int *num);


size_t reqsize = sizeof(ShmMReq);
size_t ressize = sizeof(ShmMRes);

int IfFileOpen = 0;
//static int IfSelected = 0;
//FILE *file;
FILE *listfile;
FILE *addrfile;
//FILE *file;
FILE *commfile;
int ProcessID;
int PortID;
int comm_size;
MyHashMap *SendTable;
MyHashMap *RecvTable;
MyHashMap *IrecvTable;
MyHashMap *IsendTable;
MyHashMap *IssendTable;
MyHashMap *WaitTable;
MyHashMap *ReqWaitTable;
MyList *ReduceList;
MyList *AllreduceList;
MyList *AlltoallList;
MyList *AlltoallvList;
MyList *BcastList;
MyList *AllgatherList;
MyList *GatherList;
MyHashMap *IprobeTable;
//int send_time = 0;
//int recv_time = 0;
int sleepus = 1;
int request_static = 1;
struct shmmessage *msg;
int shmid_msg;
int shmid_data;
int msgid;
int dataid;
void *buf_tmp;


int MPI_Comm_size(MPI_Comm comm, int *size)
{
	//*size = comm_size;
	//printf("mpi_comm_size, commsize is %d\n", *size);
	//printf("enter mpi_comm_size\n");
	int size_tmp;
	fread(&size_tmp,sizeof(int),1,commfile);
	*size = size_tmp;
	//printf("mpi_comm_size: comm is %d, size is %d\n", comm,  *size);
	return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{	
	//*rank = ProcessID;
	//printf("mpi_comm_rank, commsize is %d\n", *rank);
	//printf("enter mpi_comm_rank\n");
    int rank_tmp;
	fread(&rank_tmp,sizeof(int),1,commfile);
	*rank = rank_tmp;
	//printf("mpi_comm_rank, comm is %d, rank is %d\n", comm, *rank);
	return 0;
}

int MPI_Comm_split(MPI_Comm comm,int color, int key,MPI_Comm *newcomm)
{
	//printf("enter mpi_comm_split\n");
    int newcomm_tmp;
	fread(&newcomm_tmp,sizeof(int),1,commfile);
	*newcomm = newcomm_tmp;
	//printf("mpi_comm_split, comm is %d, newcomm is %d\n", comm, *newcomm);
	return 0;
}

int MPI_Comm_group(MPI_Comm comm, MPI_Group * group)
{
	//printf("enter mpi_comm_group\n");
    int group_tmp;
	fread(&group_tmp,sizeof(int),1,commfile);
	*group = group_tmp;
	//printf("leave mpi_comm_group\n");
	return 0;
}

int MPI_Group_translate_ranks(MPI_Group group1,int n,int *ranks1, MPI_Group group2,int *ranks2)
{
	//printf("enter mpi_group_translate\n");
	int n_tmp;
	fread(&n_tmp,sizeof(int),1,commfile);
	fread(ranks2,sizeof(int),n_tmp,commfile);
	//printf("leave mpi_group_translate\n");
	return 0;
}

int MPI_Group_incl(MPI_Group group,int n,int *ranks,MPI_Group *newgroup)
{
	//printf("enter mpi_group_incl\n");
    int newgroup_tmp;
	fread(&newgroup_tmp,sizeof(int),1,commfile);
	*newgroup = newgroup_tmp;
	//printf("leave mpi_group_incl\n");
	return 0;
}

int MPI_Comm_create(MPI_Comm comm,MPI_Group group,MPI_Comm *newcomm)
{
	//printf("enter mpi_comm_create\n");
    int newcomm_tmp;
	fread(&newcomm_tmp,sizeof(int),1,commfile);
	*newcomm = newcomm_tmp;
	//printf("leave mpi_comm_create\n");
	return 0;
}

int MPI_Init(int *argc, char ***argv)
{
         
	//const char *address_tmp = "127.0.0.1";
	//const char *port_tmp = "60231";
	//char* address = (char*)malloc(20);
	//char* port = (char*)malloc(10);
	////printf("message size  is %d\n", MESSAGE_SIZE);
	////printf("Req size  is %d\n", reqsize);
	/*int a=1;
	while(a)
	{
		usleep(1000);
	}*/
	printf("enter mpi_init\n");
	char address[20];
	char port[10];
	struct addrinfo *addr;
	if((listfile=fopen("ProcessList","r"))==NULL) 
	{
		return 1;
	}
	/*if((addrfile=fopen("Addr_Port_File","r"))==NULL) 
	{
		return 1;
	}*/
	int r = PMPI_Init(argc,argv);
	
	char tempList[20];
	if(fgets(tempList,128,listfile)!=NULL)
	{
		comm_size = char2int(tempList);
	}

	int id;
	PMPI_Comm_rank(MPI_COMM_WORLD,&id);
	int flag = 0;
	int strlength;
	while((fgets(tempList,128,listfile))!=NULL)
	{
		if(id==flag)
		{
			ProcessID  = char2int(tempList);
			fgets(tempList,128,listfile);
			strlength = strlen(tempList);
			strncpy(address,tempList,strlength-1);
			address[strlength-1] = '\0';
			//const char *address_tmp = address;
			//printf("address is %s\n", address);
			fgets(tempList,128,listfile);
			strlength = strlen(tempList);
			strncpy(port,tempList,strlength-1);
			port[strlength-1] = '\0';
			//const char *port_tmp = port;
			//printf("port is %s\n", port);
			break;
		}
		
		fgets(tempList,128,listfile);
		fgets(tempList,128,listfile);
		flag++;	
	}
	char filename[10];
	char ProcessID_String[10];
	int len = int2char(ProcessID,ProcessID_String);
	strcpy(filename,"comm");
    memcpy(filename+4,ProcessID_String,len);
    filename[4+len]='\0';
	printf("log name is %s\n", filename); 
	if((commfile=fopen(filename,"r"))==NULL) 
	{   
		printf("commfile fail open\n");
		return 1;
	}   
	
	//char* address_tmp = address;
        //char* port_tmp = port;
	printf("address is %s\n", address);
	printf("port is %s\n", port);
	fclose(listfile);
	msgid = 50 + ProcessID;
	dataid = 60 + ProcessID;
	printf("msgid is %d\n", msgid);
	shmid_msg = get_shmid(msgid);
	shmid_data = get_shmid(dataid);
	buf_tmp = shmat(shmid_data,NULL,0);
	msg = (struct shmmessage*)shmat(shmid_msg,NULL,0);
	printf("shmid_msg is %d\n", shmid_msg);
    /*if((fgets(tempList,128,listfile))!=NULL)
	{
		strcpy(address,tempList);
	}
    if((fgets(tempList,128,listfile))!=NULL)
	{
		strcpy(port,tempList);
	}*/

	//TEST_NZ(getaddrinfo(address, port, NULL, &addr));
	//TEST_NZ(getaddrinfo(address_tmp, port_tmp, NULL, &addr));
    //int vl = getaddrinfo(address, port, NULL, &addr);
    ////printf("returnvalue is %d\n",vl);

	SendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	RecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	WaitTable = createMyHashMap(myHashCodeRequest,myEqualRequest);
	//ReqWaitTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IrecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IsendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IssendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IprobeTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
 	ReduceList = createMyList();
	AllreduceList = createMyList();
	AlltoallList = createMyList();
	AlltoallvList = createMyList();
	AllgatherList = createMyList();
	GatherList = createMyList();
	BcastList = createMyList();
        
	printf("%d : leave mpi_init\n",ProcessID);
	return r;
}

int MPI_Send(void *buf,int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
	printf("%d : enter Send\n",ProcessID);
	//printf("%d : enter Send,dest is %d, tag is %d, type is %d, comm is %d\n",ProcessID, *dest,*tag,*datatype,*comm);

	
	msg->shmmreq.MsgType = Send;
	msg->shmmreq.datatype = datatype;
	msg->shmmreq.source = dest;
	msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;

	msg->request = 1;
	msg->response = 0;
		
 	while(msg->response == 0)
		usleep(sleepus);
	//printf("%d : leave Send\n",ProcessID);
	return msg->shmmres.returnvalue;
}

int MPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
	printf("%d :enter Isend\n", ProcessID);
	*request = request_static++;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *request;
	//printf("Isend request is %d\n", *request_tmp);
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->MsgType = Isend;
	msgl->source = dest;
	msgl->tag = tag;
	msgl->comm = comm;
	msgl->datatype = datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	//msgcws->buf = buf;
	msgcws->totalsize = 0;
	myHashMapPutData(WaitTable,request_tmp,msgl);
	myHashMapPutData(IsendTable,msgl,msgcws);
		
	//myHashMapRemoveDataByKey(IrecvTable,msgl);
	//free(msgl);
	//printf("%d :leave isend, tag is %d, source is %d, datatype is %d, count is %d\n", ProcessID, *tag, *dest, *datatype, *count);

	return 0;
	//printf("%d :leave Isend\n", ProcessID);

}

int MPI_Issend(void * buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request  *request)
{
	printf("%d :enter Issend\n", ProcessID);
	*request = request_static++;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *request;
	//printf("Isend request is %d\n", *request_tmp);
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->MsgType = Issend;
	msgl->source = dest;
	msgl->tag = tag;
	msgl->comm = comm;
	msgl->datatype = datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	//msgcws->buf = buf;
	msgcws->totalsize = 0;
	//MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
	//rbws->status = status;
	myHashMapPutData(WaitTable,request_tmp,msgl);
	myHashMapPutData(IssendTable,msgl,msgcws);
		
	//myHashMapRemoveDataByKey(IrecvTable,msgl);
	//free(msgl);
	//printf("%d :leave isend, tag is %d, source is %d, datatype is %d, count is %d\n", ProcessID, *tag, *dest, *datatype, *count);
	return 0;

}

int MPI_Recv(void *buf,int count,MPI_Datatype datatype,int source,int tag,MPI_Comm comm,MPI_Status *status)
{
	printf("%d : enter Recv\n",ProcessID);

	////printf("%d : enter Recv:source is %d, tag is %d, comm is %d ,datatype is %d,count is %d\n",ProcessID,*source,*tag,*comm,*datatype,*count);
	struct timeval start;
	gettimeofday(&start,NULL);
	int type_size;
    MPI_Type_size(datatype, &type_size);
	int length = count * type_size;
	//int sleeptime = length / BLOCK_SIZE + 1;
	int time=0;
	msg->shmmreq.MsgType = Recv;
	msg->shmmreq.datatype = datatype;
	msg->shmmreq.source = source;
	msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//printf("%d : Recv: shmid_data is %d\n",ProcessID,shmid_data);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(length > 0)
	{
		/*if(length>BUFFER_SIZE)
		{  
			shmid_data = create_shm(BUFFER_SIZE, dataid);
		}
		else
		{
			shmid_data = create_shm(length, dataid);
		}*/
		msg->shmmreq.head = head;
		msg->request = 1;
		msg->response = 0;
		while(msg->response == 0)
		{
			usleep(sleepus);
			time++;
		}
		
		memcpy(buf+head, buf_tmp, msg->shmmres.totalsize);
		
		length = length - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}
	*status = msg->shmmres.status;

	struct timeval stop;
	gettimeofday(&stop,NULL);
	long int spantime = stop.tv_sec*1000000 + stop.tv_usec - start.tv_sec * 1000000 - start.tv_usec;
	//destroy_shm(shmid_data);
	printf("%d : leave recv,time is %d, spantime is %ld\n",ProcessID,time,spantime);
	return msg->shmmres.returnvalue;
}

int MPI_Irecv(void * buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request)
{
	
	printf("%d :enter Irecv\n", ProcessID);
	*request = request_static++;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *request;
	//printf("Irecv: request is %d\n", *request_tmp);
	//int length = *count * getsize((MPI_Datatype)(*datatype));
	int type_size;
    MPI_Type_size(datatype, &type_size);
	int length = count * type_size;
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->MsgType = Irecv;
	msgl->source = source;
	msgl->tag = tag;
	msgl->comm = comm;
	msgl->datatype = datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->buf = buf;
	msgcws->totalsize = length;
	myHashMapPutData(WaitTable,request_tmp,msgl);
	myHashMapPutData(IrecvTable,msgl,msgcws);
		
	//printf("%d :leave Irecv, length is %d. tag is %d, source is %d, datatype is %d, count is %d\n, comm is %d", ProcessID, length, *tag, *source, *datatype, *count, *comm);

	//printf("%d :leave Irecv\n", ProcessID);
	return 0;

}

int MPI_Wait(MPI_Request *request, MPI_Status *status)
{	
	printf("%d: enter wait\n",ProcessID);
	int r;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *request;
	////printf("request is %d\n", *request_tmp);
  	MSGL *msgl = (MSGL *)myHashMapGetDataByKey(WaitTable,request_tmp);
	if(msgl->MsgType == Isend)
	{

		msg->shmmreq.MsgType = Isend;
		msg->shmmreq.datatype = msgl->datatype;
		msg->shmmreq.source = msgl->source;
		msg->shmmreq.tag = msgl->tag;
		msg->shmmreq.comm = msgl->comm;

		msg->request = 1;
		msg->response = 0;
		
		while(msg->response == 0)
			usleep(sleepus);

		myHashMapRemoveDataByKey(IsendTable,msgl);
	}
	else if(msgl->MsgType == Issend)
	{
		msg->shmmreq.MsgType = Issend;
		msg->shmmreq.datatype = msgl->datatype;
		msg->shmmreq.source = msgl->source;
		msg->shmmreq.tag = msgl->tag;
		msg->shmmreq.comm = msgl->comm;

		msg->request = 1;
		msg->response = 0;
		
		while(msg->response == 0)
			usleep(sleepus);

		myHashMapRemoveDataByKey(IssendTable,msgl);

	}

	else
	{
		msg->shmmreq.MsgType = Irecv;
		msg->shmmreq.datatype = msgl->datatype;
		msg->shmmreq.source = msgl->source;
		msg->shmmreq.tag = msgl->tag;
		msg->shmmreq.comm = msgl->comm;
		MSGCWS *msgcws = myHashMapGetDataByKey(IrecvTable,msgl);
		int length = msgcws->totalsize;
		int head = 0;
		//shmid_data = create_shm(BUFFER_SIZE, dataid);
		//void *buf_tmp = shmat(shmid_data,NULL,0);
		while(length > 0)
		{
			msg->shmmreq.head = head;
			msg->response = 0;
			msg->request = 1;
			while(msg->response == 0)
				usleep(sleepus);
		
			*status = msg->shmmres.status;
			memcpy(msgcws->buf + head, buf_tmp, msg->shmmres.totalsize);
		
			length = length - BUFFER_SIZE;
			head = head + BUFFER_SIZE;
		}

		//destroy_shm(shmid_data);
		myHashMapRemoveDataByKey(IrecvTable,msgl);
	}
	
	r = msg->shmmres.returnvalue;
	*status = msg->shmmres.status;
	myHashMapRemoveDataByKey(WaitTable,request_tmp);
	free(request_tmp);

	/*int sleeptime = rbws->totalsize / BLOCK_SIZE + 1;
	while(sleeptime > 0)
	{
		usleep(sleepus);
		sleeptime--;
	}*/
	////printf("%d: inside wait\n",ProcessID);
 	/*while(Message_Arrive == 0);
		usleep(sleepus);*/

	//printf("%d: leave wait return is %d\n",ProcessID, r);
	return r; 
}

int MPI_Waitall(int count, MPI_Request *array_of_requests, MPI_Status *array_of_statuses)
{
	 printf("%d: enter waitall\n",ProcessID);
	 int r;
	 
	 for(int i=0; i<count; i++)
	 {
		MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
		*request_tmp = *(array_of_requests + i);
		////printf("request is %d\n", *request_tmp);
		MSGL *msgl = (MSGL *)myHashMapGetDataByKey(WaitTable,request_tmp);
		if(msgl->MsgType == Isend)
		{

			msg->shmmreq.MsgType = Isend;
			msg->shmmreq.datatype = msgl->datatype;
			msg->shmmreq.source = msgl->source;
			msg->shmmreq.tag = msgl->tag;
			msg->shmmreq.comm = msgl->comm;

			msg->request = 1;
			msg->response = 0;
		
			while(msg->response == 0)
			usleep(sleepus);

			myHashMapRemoveDataByKey(IsendTable,msgl);
		}
		else if(msgl->MsgType == Issend)
		{
			msg->shmmreq.MsgType = Issend;
			msg->shmmreq.datatype = msgl->datatype;
			msg->shmmreq.source = msgl->source;
			msg->shmmreq.tag = msgl->tag;
			msg->shmmreq.comm = msgl->comm;

			msg->request = 1;
			msg->response = 0;
		
			while(msg->response == 0)
				usleep(sleepus);

			myHashMapRemoveDataByKey(IssendTable,msgl);

		}

		else
		{
			msg->shmmreq.MsgType = Irecv;
			msg->shmmreq.datatype = msgl->datatype;
			msg->shmmreq.source = msgl->source;
			msg->shmmreq.tag = msgl->tag;
			msg->shmmreq.comm = msgl->comm;
			MSGCWS *msgcws = myHashMapGetDataByKey(IrecvTable,msgl);
			int length = msgcws->totalsize;
			int head = 0;
			//shmid_data = create_shm(BUFFER_SIZE, dataid);
			//void *buf_tmp = shmat(shmid_data,NULL,0);
			while(length > 0)
			{
				msg->shmmreq.head = head;
				msg->response = 0;
				msg->request = 1;
				while(msg->response == 0)
					usleep(sleepus);
		
				memcpy(msgcws->buf + head, buf_tmp, msg->shmmres.totalsize);
		
				length = length - BUFFER_SIZE;
				head = head + BUFFER_SIZE;
			}

			//destroy_shm(shmid_data);
			myHashMapRemoveDataByKey(IrecvTable,msgl);
		}
	
		r = msg->shmmres.returnvalue;
		*(array_of_statuses + i) = msg->shmmres.status;
		myHashMapRemoveDataByKey(WaitTable,request_tmp);
		free(request_tmp);

	 }
	 //printf("%d: leave waitall\n",ProcessID);
	 return r;
}

int MPI_Sendrecv(void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, MPI_Comm comm, MPI_Status *status)
{	
	printf("%d: enter sendrecv\n",ProcessID);
	int recv_type_size;
	//int send_type_size;
    MPI_Type_size(recvtype, &recv_type_size);
    //MPI_Type_size(sendtype, &send_type_size);
	//int sendlength = sendcount * send_type_size;
	int recvlength = recvcount * recv_type_size;
	//int sleeptime = length / BLOCK_SIZE + 1;
	msg->shmmreq.MsgType = Recv;
	msg->shmmreq.datatype = recvtype;
	msg->shmmreq.source = source;
	msg->shmmreq.tag = recvtag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}
	*status = msg->shmmres.status;

	int r = msg->shmmres.returnvalue;         	
	//printf("%d : leave recv\n",ProcessID);
	destroy_shm(shmid_data);
	return r;
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status)
{
	printf("%d: enter Iprove\n",ProcessID);
	msg->shmmreq.MsgType = Iprobe;
	//msg->shmmreq.datatype = datatype;
	msg->shmmreq.source = source;
	msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;

	msg->request = 1;
	msg->response = 0;
	
	while(msg->response == 0)
		usleep(sleepus);

	*flag = msg->shmmres.totalsize;
	*status = msg->shmmres.status;
	//printf("%d: leave Iprove\n",ProcessID);
	return msg->shmmres.returnvalue;
}

int MPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
	printf("%d: enter Alltoall\n",ProcessID);
    	
	int procNum; 
	get_comm_num(comm, &procNum);
	//int sendlength = procNum * (*sendcount) * getsize((MPI_Datatype)(*sendtype));
	//int recvlength = procNum * (*recvcount) * getsize((MPI_Datatype)(*recvtype));
	int send_type_size;
	int recv_type_size;
    MPI_Type_size(sendtype, &send_type_size);
    MPI_Type_size(recvtype, &recv_type_size);
	//int sendlength = procNum * (*sendcount) * send_type_size;
	//int recvlength = procNum * (*recvcount) * recv_type_size;
	int sendlength = procNum * 256 * send_type_size;
	int recvlength = procNum * 256 * recv_type_size;
	//printf("%d: Alltoall recvlength is %d, recvcount is %d, sendcount is %d, recv_type_size is %d\n",ProcessID,recvlength,*recvcount,*sendcount,recv_type_size);
	
	//printf("%d: alltoall out while, Message_arrive is %d\n",ProcessID,Message_Arrive);
	msg->shmmreq.MsgType = Alltoall;
	msg->shmmreq.datatype = recvtype;
	//msg->shmmreq.source = source;
	//msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}

	
	int r = msg->shmmres.returnvalue;         	

	//destroy_shm(shmid_data);
	//printf("%d: alltoall returnvalue is %d\n",ProcessID,r);
	return r;
}

int MPI_Alltoallv(void* sendbuf, int *sendcounts, int *sdispls, MPI_Datatype sendtype, void* recvbuf, int *recvcounts, int *rdispls, MPI_Datatype recvtype, MPI_Comm comm)
{	

	printf("%d: enter Alltoallv\n",ProcessID);
	int procNum; 
	get_comm_num(comm, &procNum);
    int sendcount = 0;
	int recvcount = 0;
	for (int i=0;i<procNum;i++)
	{
		sendcount = sendcount + sendcounts[i];
		recvcount = recvcount + recvcounts[i];
	}
	/*for (int j=0;j<procNum;j++)
	{
		recvcount = recvcount + recvcounts[j];
	}*/
	int send_type_size;
	int recv_type_size;
    MPI_Type_size(sendtype, &send_type_size);
    MPI_Type_size(recvtype, &recv_type_size);
	int sendlength = sendcount * send_type_size;
	int recvlength = recvcount * recv_type_size;
	////printf("%d: Alltoallv sendcount is %d,recvcount is %d\n",ProcessID,sendcount,recvcount);
	////printf("%d: Alltoallv sendlength is %d,recvlength is %d\n",ProcessID,sendlength,recvlength);
	msg->shmmreq.MsgType = Alltoallv;
	msg->shmmreq.datatype = recvtype;
	//msg->shmmreq.source = source;
	//msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}

	
	int r = msg->shmmres.returnvalue;         	
	
	
	//destroy_shm(shmid_data);
	//int r = *returnvalue;         	
	//printf("%d: leave  alltoallv\n",ProcessID);
	return r;
}


int MPI_Allreduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, int op, MPI_Comm comm)
{
	printf("%d: enter Allreduce\n",ProcessID);
	//int sendlength = (*count) * getsize((MPI_Datatype)(*datatype));
	//int recvlength = (*count) * getsize((MPI_Datatype)(*datatype));
	int type_size;
    MPI_Type_size(datatype, &type_size);
	int sendlength = count * type_size;
	int recvlength = sendlength;
	////printf("%d: Allreduce recvlength is %d\n",ProcessID,recvlength);
	msg->shmmreq.MsgType = Allreduce;
	msg->shmmreq.datatype = datatype;
	//msg->shmmreq.source = source;
	//msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}

	
	int r = msg->shmmres.returnvalue;         	

	//printf("%d: allreduce returnvalue is %d\n",ProcessID,*ierr);

	//destroy_shm(shmid_data);
	//printf("%d: leave Allreduce\n",ProcessID);
	return r;
}	

int MPI_Reduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, int op, int root, MPI_Comm comm)
{	
	printf("%d: enter reduce\n",ProcessID);
	int r;
	if(ProcessID == root)
	{
		int type_size;
		MPI_Type_size(datatype, &type_size);
		int recvlength = count * type_size;
		msg->shmmreq.MsgType = Reduce;
		msg->shmmreq.datatype = datatype;
		//msg->shmmreq.source = source;
		//msg->shmmreq.tag = tag;
		msg->shmmreq.comm = comm;
	
		int head = 0;
		//shmid_data = create_shm(BUFFER_SIZE, dataid);
		//void *buf_tmp = shmat(shmid_data,NULL,0);
		while(recvlength > 0)
		{
			msg->shmmreq.head = head;
			msg->response = 0;
			msg->request = 1;
			while(msg->response == 0)
				usleep(sleepus);
		
			memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
			recvlength = recvlength - BUFFER_SIZE;
			head = head + BUFFER_SIZE;
		}

		destroy_shm(shmid_data);
	

	}
	else
	{
		msg->shmmreq.MsgType = Reduce_NR;
		msg->shmmreq.datatype = datatype;
		//msg->shmmreq.source = dest;
		//msg->shmmreq.tag = tag;
		msg->shmmreq.comm = comm;

		msg->request = 1;
		msg->response = 0;
		
		while(msg->response == 0)
			usleep(sleepus);


	}
	
	r = msg->shmmres.returnvalue;         	
	////printf("reduce returnvalue is %d\n",r);
	//printf("%d: leave reduce\n",ProcessID);
	return r;
}

int MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
	printf("%d: enter Bcast\n",ProcessID);
	int procNum; 
	get_comm_num(comm, &procNum);
    int type_size;
	MPI_Type_size(datatype, &type_size);
	int sendlength = count * type_size;
	int recvlength = sendlength;
	msg->shmmreq.MsgType = Bcast;
	msg->shmmreq.datatype = datatype;
	//msg->shmmreq.source = source;
	//msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(buffer + head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}

	
	int r = msg->shmmres.returnvalue;         	
	//destroy_shm(shmid_data);

	//printf("%d: leave Bcast\n",ProcessID);
	return r;
}

int MPI_Allgather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
	printf("%d: enter Allgather\n",ProcessID);
	int recv_type_size;
    MPI_Type_size(recvtype, &recv_type_size);
	int send_type_size;
    MPI_Type_size(sendtype, &send_type_size);
	int sendlength = sendcount * send_type_size;
	int recvlength = recvcount * recv_type_size;
	////printf("%d: Allreduce recvlength is %d\n",ProcessID,recvlength);
	msg->shmmreq.MsgType = Allgather;
	msg->shmmreq.datatype = recvtype;
	//msg->shmmreq.source = source;
	//msg->shmmreq.tag = tag;
	msg->shmmreq.comm = comm;
	
	int head = 0;
	//shmid_data = create_shm(BUFFER_SIZE, dataid);
	//void *buf_tmp = shmat(shmid_data,NULL,0);
	while(recvlength > 0)
	{
		msg->shmmreq.head = head;
		msg->response = 0;
		msg->request = 1;
		while(msg->response == 0)
			usleep(sleepus);
		
		memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
		recvlength = recvlength - BUFFER_SIZE;
		head = head + BUFFER_SIZE;
	}

	
	int r = msg->shmmres.returnvalue;         	

	

	//printf("%d: allreduce returnvalue is %d\n",ProcessID,*ierr);
	destroy_shm(shmid_data);

	//printf("%d: leave Allgather\n",ProcessID);
	return r;

}

int MPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
	printf("%d: enter gather\n",ProcessID);
	int r;
	//int sendlength = (*count) * getsize((MPI_Datatype)(*datatype));
	int recv_type_size;
    MPI_Type_size(recvtype, &recv_type_size);
	
	if(ProcessID == root)
	{
		
		int recvlength = recv_type_size * recvcount;
		msg->shmmreq.MsgType = Gather;
		msg->shmmreq.datatype = recvtype;
		//msg->shmmreq.source = source;
		//msg->shmmreq.tag = tag;
		msg->shmmreq.comm = comm;
	
		int head = 0;
		//shmid_data = create_shm(BUFFER_SIZE, dataid);
		//void *buf_tmp = shmat(shmid_data,NULL,0);
		while(recvlength > 0)
		{
			msg->shmmreq.head = head;
			msg->response = 0;
			msg->request = 1;
			while(msg->response == 0)
				usleep(sleepus);
		
			memcpy(recvbuf+head, buf_tmp, msg->shmmres.totalsize);
		
			recvlength = recvlength - BUFFER_SIZE;
			head = head + BUFFER_SIZE;
		}

		//destroy_shm(shmid_data);
	
	
	}
	else
	{
		msg->shmmreq.MsgType = Reduce_NR;
		msg->shmmreq.datatype = recvtype;
		//msg->shmmreq.source = dest;
		//msg->shmmreq.tag = tag;
		msg->shmmreq.comm = comm;

		msg->request = 1;
		msg->response = 0;
		
		while(msg->response == 0)
			usleep(sleepus);

	}
	
	//printf("%d: leave gather\n",ProcessID);
	return r;

}

int MPI_Finalize()
{
	//printf("%d: enter finalize\n",ProcessID);
	
	
 	freeMyHashMap(IsendTable);
 	freeMyHashMap(IssendTable);
    freeMyHashMap(IrecvTable);
	freeMyHashMap(WaitTable);
	int r = PMPI_Finalize();
	return r;
}

int get_comm_num(MPI_Comm comm, int *num)
{
	*num = comm_size;
	return 0;
}
