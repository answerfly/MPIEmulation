#include"myMessageWithTime.h"
#include"myHashMap.h"
#include"myList.h"
int ProcessSend(RMR *rmr,MyHashMap *table);
int ProcessRecv(RMR *rmr,MyHashMap *table, void *buf);
int ProcessIsend(RMR *rmr, MyHashMap *IsendTable);
int ProcessIssend(RMR *rmr, MyHashMap *IssendTable);
int ProcessIrecv(RMR *rmr, MyHashMap *IrecvTable, void *buf);
int ProcessReduce(RMR *rmr, MyList *list, void *buf);
int ProcessAllreduce(RMR *rmr, MyList *list, void *buf);
int ProcessAlltoall(RMR *rmr, MyList *list, void *buf);
int ProcessAlltoallv(RMR *rmr, MyList *list, void *buf);
int ProcessIprobe(RMR *rmr, MyHashMap *IprobeTable);
int ProcessGather(RMR *rmr, MyList *GatherList, void *buf);
int ProcessAllgather(RMR *rmr, MyList *AllgatherList, void *buf);

