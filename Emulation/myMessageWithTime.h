#include"mpi.h"
#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>

typedef enum MessageType
{
	Start,
	Stop,
	ReadMsg,
	Send,
	Isend,
	Recv,
	Irecv,
	Alltoall,
	Alltoallv,
	Reduce,
	Reduce_NR,
	Allreduce,
	Bcast,
	Sendrecv,
	Issend,
	Gather,
	Gather_NR,
	Allgather,
	Iprobe,
	None
}MT;

typedef struct RecvMessageRecord
{
    MT MsgType;
	//int count;
	//MPI_Datatype sendtype;
	MPI_Datatype datatype;
	int typesize;
	//int dest;
	int source;
	int tag;
	MPI_Comm comm;
	MPI_Status status;
	//MPI_Request request;
	//void *buf;
        int sendlength;
	int recvcount;
	int returnvalue;
	//long int time;
	double time;
}RMR;




typedef struct RdmaMessageResponse_Without_Status
{
	long int time;
	int returnvalue;
}RdmaMResWos;

typedef struct RdmaMessageResponse_With_Status
{
	long int time;
	int returnvalue;
	MPI_Status status;
}RdmaMResWs;

typedef struct RdmaMessageRequest
{
	MT MsgType;
	MPI_Datatype datatype;
	int source;
	int tag;
	MPI_Comm comm;
	int head;
	//int size;
	int totalsize;
}RdmaMReq;

typedef struct RdmaMessageResponse
{
	MT MsgType;
	double time;
	int returnvalue;
	MPI_Status status;
	int source;
	int tag;
	MPI_Comm comm;
	MPI_Datatype datatype;
	int totalsize;
	int head;
	//int size;
}RdmaMRes;


typedef struct ShmMessageRequest
{
	MT MsgType;
	MPI_Datatype datatype;
	int source;
	int tag;
	MPI_Comm comm;
	int head;
	//int size;
	int totalsize;
}ShmMReq;


typedef struct ShmMessageResponse
{
	MT MsgType;
	double time;
	int returnvalue;
	MPI_Status status;
	int source;
	int tag;
	MPI_Comm comm;
	MPI_Datatype datatype;
	int totalsize;
	int head;
	//int size;
}ShmMRes;

/*struct message
{
    struct 
    {
      uint64_t addr;
      uint32_t rkey;
    } mr;
    RdmaMReq rdmamreq;
};*/

typedef struct RdmaBufferWithStatus
{
	MPI_Status *status;
	void *buf;
	int *returnvalue;
	int totalsize;
}RBWS;

typedef struct RdmaBufferWithoutStatus
{
	void *buf;
	int *returnvalue;
	int totalsize;
}RBWoS;

typedef struct RdmaStatusAndReturnvalue
{
	MPI_Status status;
	int returnvalue;
}RSaR;

typedef struct MessageLable
{
	MT MsgType; 
	int source;
	int tag;
	MPI_Comm comm;
	MPI_Datatype datatype;
}MSGL;

typedef struct MessageContentWithStatus
{
	MPI_Status status;
	int totalsize;
    int returnvalue;
	void *buf;	
	double time;
}MSGCWS;

typedef struct MessageContentWithoutStatus
{
    int returnvalue;
	int totalsize;
	void *buf;	
	double time;
}MSGCWOS;

typedef struct BufAndRequest
{
	void *buf;	
	MPI_Request *request;
}BaR;

typedef struct TmpMessageRecord_ForRecord
{
        MT MsgType;
	int count;
	//MPI_Datatype sendtype;
	MPI_Datatype datatype;
	int typesize;
	//int dest;
	int source;
	int tag;
	MPI_Comm comm;
	//MPI_Request request;
	void *buf;
        //int sendcount;
	int returnvalue;
	long int time;
}TMR;

typedef struct TmpMessageRecord_ForSimulate
{
	MPI_Datatype datatype;
	int totalsize;
	int source;
	int tag;
	MPI_Comm comm;
	void *buf;
}TMRS;

int getsize(MPI_Datatype datatype);
int int2char(int id,char *idString);
int char2int(char *ProcessID_String);
int IfSameNode(int processNum, int *processList, int number);
