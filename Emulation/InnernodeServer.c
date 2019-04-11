#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include"mpi.h"
//#include"myMessage.h"
#include"myHashMap.h"
#include"myHashCode.h"
#include"myEqual.h"
#include"myList.h"
#include"processMsgRdma.h"
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

const size_t BUFFER_SIZE = 10 * 1024 * 1024;
const size_t BLOCK_SIZE = 10 * 1024 * 1024 - sizeof(ShmMRes);
const size_t BUFFER_SIZE_SMALL = 8 * 1024;
const size_t BLOCK_SIZE_SMALL = 8 * 1024 - sizeof(ShmMRes);
const size_t MESSAGE_SIZE = sizeof(struct shmmessage) ;




static void die(const char *reason);






const size_t reqsize = sizeof(ShmMReq);
const size_t ressize = sizeof(ShmMRes);
int communication_num = 0;
int communication_current = 1;
int expectedRecvNum[256];
int ReadOneMessage();
int PassOneMessage();
int ProcessShmRequest(ShmMReq shmmreq);
FILE *file;
MyHashMap *SendTable;
MyHashMap *IsendTable;
MyHashMap *RecvTable;
MyHashMap *IrecvTable;
MyHashMap *WaitTable;
MyHashMap *IssendTable;
MyHashMap *IprobeTable;
MyList *ReduceList;
MyList *AllreduceList;
MyList *AlltoallList;
MyList *AlltoallvList;
MyList *BcastList;
MyList *GatherList;
MyList *AllgatherList;
int send_time = 0;
int recv_time = 0;
int ifReadFile = 1;
int shmid_msg;
int shmid_data;
int msgid;
int dataid;
struct shmmessage *msg;
int sleepus = 50;

int main(int argc,char **argv)
{
	
        ////printf("MESSAGE_SIZE is %d\n", MESSAGE_SIZE);	
        ////printf("req size is %d\n", reqsize);	
	char filename[10];
	strcpy(filename,"log");
    memcpy(filename+3,argv[1],strlen(argv[1]));
    filename[3+strlen(argv[1])]='\0';
    //printf("log name is %s\n", filename);	
	//printf("Message size is %d\n", sizeof(RMR));
    if((file=fopen(filename,"r"))==NULL) 
	{
		//IfFileOpen = 1;
		//printf("file is not exist\n");	
		return 0;
	}
	
	SendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IsendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	RecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IrecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IssendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IprobeTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	//WaitTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
 	ReduceList = createMyList();
	AllreduceList = createMyList();
	AlltoallList = createMyList();
	AlltoallvList = createMyList();
	BcastList = createMyList();
	GatherList = createMyList();
	AllgatherList = createMyList();
	//int recv_num;
	//while(1)
	for (int i=0; i<0; i++)
	{
		int r = ReadOneMessage();

		if(r==0)
		{	
			ifReadFile = 0;
			break;
		}

	}
	//fclose(file);
	////printf("communication_num is %d\n", communication_num);
	
	
	/*MyNode *p = AlltoallvList->first;
	while (p)  
	{  
	        MSGCWOS *msgcwos = (MSGCWOS *)p->data;
	        //printf("totalsize is %d\n", msgcwos->totalsize);	
	        p = p->next;  
        }*/
	/*//printf("SendTable size is %d\n", myHashMapGetSize(SendTable));
	MyHashMapEntryIterator* iterator = createMyHashMapEntryIterator(RecvTable);
        while (myHashMapEntryIteratorHasNext(iterator)) {  
		Entry * pp= myHashMapEntryIteratorNext(iterator);
	    	MSGL * mykey = pp-> key;
	        MSGCWS * myvalue = pp->value;
		//printf("source is %d, tag is %d, comm is %d\n",mykey->source,mykey->tag,mykey->comm);
		//printf("return value is %d, totalsize is %d\n",myvalue->returnvalue,myvalue->totalsize);
		if(myvalue->totalsize == 4)
		{
			int *myvalue2 = (int *)myvalue->buf;
			//printf("value is %d\n", *myvalue2);
		}
	}*/ 
	  	
	char port[5] = "50000";
	memcpy(port+5-strlen(argv[1]),argv[1],strlen(argv[1]));

	
	msgid = 50 + char2int(argv[1]);
	dataid = 60 + char2int(argv[1]);
	shmid_msg = create_shm(sizeof(struct shmmessage),msgid);
	msg = (struct shmmessage *)shmat(shmid_msg,NULL,0);
	msg->request = 0;
	printf("listening...%d\n", shmid_msg);
	
	while(1)
	{
		while(msg->request == 0	)
		{
			usleep(50);
			continue;
		}
		
		//printf("get msg\n");
		if(ProcessShmRequest(msg->shmmreq) == 0)
		  break;
	}
	
	freeMyHashMap(SendTable);
	freeMyHashMap(RecvTable);
	freeMyHashMap(IrecvTable);
	freeMyList(ReduceList);
	freeMyList(AllreduceList);
	freeMyList(AlltoallList);
	freeMyList(AlltoallvList);
	freeMyList(BcastList);
	freeMyList(GatherList);
	freeMyList(AllgatherList);
	return 1;
}

int ReadOneMessage()
{
	//printf("enter read one node\n");
	RMR *rmr = (RMR *)malloc(sizeof(RMR));
	if(fread(rmr,sizeof(RMR),1,file)==0)
	{
		free(rmr);
		return 0;
	}
	//printf("read one node, type is %d\n", rmr->MsgType);
	////printf("type is %d\n", rmr->MsgType);
	//MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	/*if(rmr->sendlength <= BLOCK_SIZE)
		*recvNum = 1;
	else
		*recvNum = rmr->sendlength / BLOCK_SIZE + 2 ;*/
	void *buf = malloc(rmr->typesize * rmr->recvcount);
	int r;
	switch(rmr->MsgType)
	{
		case Send:
			//printf("message type is send\n");
			r=ProcessSend(rmr,SendTable);
			free(buf);
		 	break;
		case Recv:			
			//printf("message type is recv,typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessRecv(rmr,RecvTable,buf);
			break;
		case Irecv:
			//printf("message type is Irecv\n");
			//printf("message type is Irecv,typesize is %d, recvcount is %d, source is %d, tag is %d, comm is %d, datatype is %d\n",rmr->typesize,rmr->recvcount,rmr->source, rmr->tag, rmr->comm, rmr->datatype);
			fread(buf,rmr->typesize,rmr->recvcount,file);
			r=ProcessIrecv(rmr,IrecvTable,buf);
			//printf("readlog :leave Irecv\n");
			break;
		case Isend:
			//printf("message type is Isend\n");
			r=ProcessIsend(rmr,IsendTable);
			//free(buf);
			break;
		case Issend:
			r=ProcessIssend(rmr,IssendTable);
			free(buf);
			break;
		case Iprobe:
			r=ProcessIprobe(rmr,IprobeTable);
			free(buf);
			break;
		case Reduce:
			//printf("message type is reduce, typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessReduce(rmr,ReduceList,buf);
			break;
		case Reduce_NR:
			//printf("message type is Reduce_NR\n");
			r=ProcessReduce(rmr,ReduceList,NULL);
			break;
		case Allreduce:
			//printf("message type is Allreduce,recvcount is %d\n", rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessAllreduce(rmr,AllreduceList,buf);
			break;
		case Alltoall:
			//printf("message type is Alltoall,typesize is %d, recvcount is %d\n", rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessAlltoall(rmr,AlltoallList,buf);
			break;
		case Alltoallv:
			//printf("message type is Alltoallv\n");
			//printf("message type is Alltoallv,typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessAlltoallv(rmr,AlltoallvList,buf);
			break;
		case Bcast:
			//printf("message type is Bcast,typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			r=ProcessAlltoallv(rmr,BcastList,buf);			
			break;
		case Gather:
			fread(buf,rmr->typesize,rmr->recvcount,file);
			r=ProcessGather(rmr,GatherList,buf);
			break;
		case Gather_NR:
			r=ProcessGather(rmr,GatherList,NULL);
			break;
		case Allgather:
			fread(buf,rmr->typesize,rmr->recvcount,file);
			r=ProcessAllgather(rmr,AllgatherList,buf);
			break;
		default:
			//printf("message type is none\n");
			free(buf);
			break;
	}
	return 1;
}


int PassOneMessage()
{
	//printf("enter pass one msg\n");
	RMR *rmr = (RMR *)malloc(sizeof(RMR));
	if(fread(rmr,sizeof(RMR),1,file)==0)
	{
		free(rmr);
		return 0;
	}
	//printf("pass one msg, type is %d\n", rmr->MsgType);
	//MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	/*if(rmr->sendlength <= BLOCK_SIZE)
		*recvNum = 1;
	else
		*recvNum = rmr->sendlength / BLOCK_SIZE + 2 ;*/
	void *buf = malloc(rmr->typesize * rmr->recvcount);
	switch(rmr->MsgType)
	{
		case Send:
			//printf("message type is send\n");
		 	break;
		case Recv:			
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Irecv:
			//printf("message type is Irecv\n");
			//printf("message type is Irecv,typesize is %d, recvcount is %d, source is %d, tag is %d, comm is %d, datatype is %d\n",rmr->typesize,rmr->recvcount,rmr->source, rmr->tag, rmr->comm, rmr->datatype);
			fread(buf,rmr->typesize,rmr->recvcount,file);
			//printf("readlog :leave Irecv\n");
			//printf("readlog :leave Irecv\n");
			break;
		case Isend:
			//printf("message type is Isend\n");
			break;
		case Issend:
			break;
		case Iprobe:
			break;
		case Reduce:
			//printf("message type is reduce, typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Reduce_NR:
			//printf("message type is Reduce_NR\n");
			break;
		case Allreduce:
			//printf("message type is Allreduce,recvcount is %d\n", rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Alltoall:
			//printf("message type is Alltoall,typesize is %d, recvcount is %d\n", rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Alltoallv:
			//printf("message type is Alltoallv\n");
			//printf("message type is Alltoallv,typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Bcast:
			//printf("message type is Bcast,typesize is %d, recvcount is %d\n",rmr->typesize,rmr->recvcount);
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Gather:
			fread(buf,rmr->typesize,rmr->recvcount,file);
			break;
		case Gather_NR:
			break;
		case Allgather:
			fread(buf,rmr->typesize,rmr->recvcount,file);
			break;
		default:
			break;
	}
	free(buf);
	//printf("exit pass one msg\n");
	return 1;
}

int ProcessShmRequest(ShmMReq shmmreq)
{
	int r = 1;
	//printf("Process msg, type is %d\n", shmmreq.MsgType);
	if(shmmreq.MsgType == ReadMsg)
	{
		r = PassOneMessage();
		msg->response = 1;
		msg->request = 0;
		return r;
	}
	//printf("type is %d\n", shmmreq.MsgType);
	if(shmmreq.head == 0)
		if(ReadOneMessage() == 0) 
			return 0;
    	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	MSGCWS *msgcws;
	MSGCWOS *msgcwos;
	void *buf;
	switch(shmmreq.MsgType)
	{
		case Send:
			//printf("request type is send\n");
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = shmmreq.datatype;
			msgcwos = (MSGCWOS *)myHashMapGetDataByKey(SendTable,msgl);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			myHashMapRemoveDataByKey(SendTable,msgl);
		 	break;
		case Recv:			
			//printf("request type is recv\n");
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = shmmreq.datatype;
			msgcws = (MSGCWS *)myHashMapGetDataByKey(RecvTable,msgl);
			msg->shmmres.status = msgcws->status;
			msg->shmmres.returnvalue = msgcws->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
				//printf("recv, shmid_data is %d\n", shmid_data);
			}
            		if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcws->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcws->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcws->buf);
				myHashMapRemoveDataByKey(RecvTable,msgl);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcws->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Irecv:
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = shmmreq.datatype;
			msgcws = (MSGCWS *)myHashMapGetDataByKey(IrecvTable,msgl);
			msg->shmmres.status = msgcws->status;
			msg->shmmres.returnvalue = msgcws->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcws->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcws->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcws->buf);
				myHashMapRemoveDataByKey(IrecvTable,msgl);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcws->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Isend:
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = shmmreq.datatype;
			msgcws = (MSGCWS *)myHashMapGetDataByKey(IsendTable,msgl);
			msg->shmmres.returnvalue = msgcws->returnvalue;
			msg->shmmres.status = msgcws->status;
			myHashMapRemoveDataByKey(IsendTable,msgl);
			break;
		case Issend:
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = shmmreq.datatype;
			msgcws = (MSGCWS *)myHashMapGetDataByKey(IssendTable,msgl);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			msg->shmmres.status = msgcws->status;
			myHashMapRemoveDataByKey(IssendTable,msgl);
			break;
		case Iprobe:
			msgl->source = shmmreq.source;
			msgl->tag = shmmreq.tag;
			msgl->comm = shmmreq.comm;
			msgl->datatype = MPI_INT;
			msgcws = (MSGCWS *)myHashMapGetDataByKey(IprobeTable,msgl);
			msg->shmmres.returnvalue = msgcws->returnvalue;
			msg->shmmres.status = msgcws->status;
			msg->shmmres.totalsize = msgcws->totalsize;
			myHashMapRemoveDataByKey(IprobeTable,msgl);
			break;
		case Reduce:
			msgcwos = myListGetDataAtFirst(ReduceList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcws->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(ReduceList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Reduce_NR:
			msgcwos = myListGetDataAtFirst(ReduceList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			myListRemoveDataAtFirst(ReduceList);
			break;
		case Allreduce:
			msgcwos = myListGetDataAtFirst(AllreduceList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcws->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(AllreduceList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Alltoall:
			msgcwos = myListGetDataAtFirst(AlltoallList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcwos->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcwos->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(AlltoallList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Alltoallv:
			msgcwos = myListGetDataAtFirst(AlltoallvList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcwos->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcwos->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(AlltoallvList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Bcast:
			msgcwos = myListGetDataAtFirst(BcastList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcws->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(BcastList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcws->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Gather:
			msgcwos = myListGetDataAtFirst(GatherList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			if(shmmreq.head == 0)
			{
				shmid_data = get_shmid(dataid);
				buf = shmat(shmid_data,NULL,0);
			}
            if((msgcws->totalsize - msg->shmmreq.head) < BLOCK_SIZE)
			{
				msg->shmmres.totalsize = msgcwos->totalsize - msg->shmmreq.head;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
				free(msgcwos->buf);
				myListRemoveDataAtFirst(GatherList);
			}
			else
			{
				msg->shmmres.totalsize = BUFFER_SIZE;
				memcpy(buf, msgcwos->buf + msg->shmmreq.head, msg->shmmres.totalsize);
			}
			break;
		case Gather_NR:
			msgcwos = myListGetDataAtFirst(GatherList);
			msg->shmmres.returnvalue = msgcwos->returnvalue;
			myListRemoveDataAtFirst(GatherList);
			break;
		default:
			break;
	}
	msg->response = 1;
	msg->request = 0;
	return r;
}


void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}
