#include"processMsgRdma.h"
#include"mpi.h"
//#include"myMessage.h"
//#include"myHashMap.h"
#include"myHashCode.h"
#include"myEqual.h"
#include<stdlib.h>
//#include"myList.h"

int ProcessSend(RMR *rmr,MyHashMap *SendTable)
{	
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->tag = rmr->tag;
	msgl->source = rmr->source;
	msgl->comm = rmr->comm;
	msgl->datatype = rmr->datatype;
	//printf("tag is %d,source is %d\n",msgl->tag,msgl->source);
	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myHashMapPutData(SendTable,msgl,msgcwos);
	return 1;
}

int ProcessIsend(RMR *rmr,MyHashMap *IsendTable)
{	
	
	MSGL *msgl_w = (MSGL *)malloc(sizeof(MSGL));
	msgl_w->tag = rmr->tag;
	msgl_w->source = rmr->source;
	msgl_w->comm = rmr->comm;
	msgl_w->datatype = rmr->datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->status = rmr->status;
	msgcws->returnvalue = rmr->returnvalue;
	msgcws->totalsize = rmr->recvcount * rmr->typesize;
	msgcws->time = rmr->time;
	myHashMapPutData(IsendTable,msgl_w,msgcws);
	return 1;
}

int ProcessIssend(RMR *rmr,MyHashMap *IssendTable)
{	
	
	MSGL *msgl_w = (MSGL *)malloc(sizeof(MSGL));
	msgl_w->tag = rmr->tag;
	msgl_w->source = rmr->source;
	msgl_w->comm = rmr->comm;
	msgl_w->datatype = rmr->datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->status = rmr->status;
	msgcws->returnvalue = rmr->returnvalue;
	msgcws->totalsize = rmr->recvcount * rmr->typesize;
	msgcws->time = rmr->time;
	myHashMapPutData(IssendTable,msgl_w,msgcws);
	return 1;
}

int ProcessRecv(RMR *rmr,MyHashMap *table, void *buf)
{
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->tag = rmr->tag;
	msgl->source = rmr->source;
	msgl->comm = rmr->comm;
	msgl->datatype = rmr->datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->status = rmr->status;
	msgcws->returnvalue = rmr->returnvalue;
	msgcws->buf = buf;
	msgcws->totalsize = rmr->recvcount * rmr->typesize;
	//printf("ProcessRecv returnvalue is %d, totalsize is %d\n",msgcws->returnvalue,msgcws->totalsize);
	msgcws->time = rmr->time;
	myHashMapPutData(table,msgl,msgcws);
	return 1;
}

int ProcessIrecv(RMR *rmr,MyHashMap *IrecvTable, void *buf)
{	
	
	MSGL *msgl_w = (MSGL *)malloc(sizeof(MSGL));
	msgl_w->tag = rmr->tag;
	msgl_w->source = rmr->source;
	msgl_w->comm = rmr->comm;
	msgl_w->datatype = rmr->datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->status = rmr->status;
	msgcws->returnvalue = rmr->returnvalue;
	msgcws->buf = buf;
	msgcws->totalsize = rmr->recvcount * rmr->typesize;
	msgcws->time = rmr->time;
	myHashMapPutData(IrecvTable,msgl_w,msgcws);
	return 1;
}

int ProcessIprobe(RMR *rmr, MyHashMap *IprobeTable)
{
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->tag = rmr->tag;
	msgl->source = rmr->source;
	msgl->comm = rmr->comm;
	msgl->datatype = rmr->datatype;
	MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
	msgcws->status = rmr->status;
	msgcws->returnvalue = rmr->returnvalue;
	msgcws->totalsize = rmr->recvcount;
	msgcws->time = rmr->time;
	myHashMapPutData(IprobeTable,msgl,msgcws);
		
}

int ProcessReduce(RMR *rmr,MyList *list, void *buf)
{

	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;
}

int ProcessAllreduce(RMR *rmr, MyList *list, void *buf)
{

	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;
}

int ProcessAlltoall(RMR *rmr,MyList *list, void *buf)
{

	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;
}

int ProcessAlltoallv(RMR *rmr,MyList *list, void *buf)
{

	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;
}

int ProcessGather(RMR *rmr,MyList *list, void *buf)
{
	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;


}


int ProcessAllgather(RMR *rmr,MyList *list, void *buf)
{
	MSGCWOS *msgcwos = (MSGCWOS *)malloc(sizeof(MSGCWOS));
	msgcwos->returnvalue = rmr->returnvalue;
	msgcwos->buf = buf;
	msgcwos->totalsize = rmr->recvcount * rmr->typesize;
	msgcwos->time = rmr->time;
	myListInsertDataAtLast(list,msgcwos);
	return 1;
}
