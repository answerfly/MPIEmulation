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
#include <rdma/rdma_cma.h>
#include "shm_common.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)


struct message
{
    struct 
    {
      uint64_t addr;
      uint32_t rkey;
    } mr;
    RdmaMReq rdmamreq;
};

struct shmmessage
{
        int request;
        int response;
        ShmMReq shmmreq;
        ShmMRes shmmres;
};

const size_t BUFFER_SIZE = 10 * 1024 * 1024;
const size_t BLOCK_SIZE = 10 * 1024 * 1024 - sizeof(RdmaMRes);
const size_t BUFFER_SIZE_SMALL = 1024 * 8;
const size_t BLOCK_SIZE_SMALL = 1024 * 8 -sizeof(RdmaMRes);
const size_t MESSAGE_SIZE = sizeof(struct message);
const int TIMEOUT_IN_MS = 500; /* ms */
int Message_Arrive = 0;
int nodesize = 16;


struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct client_context {
  //struct rdma_cm_id *id;
  //struct ibv_qp *qp;
  
  struct message *msg;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  void *recv_region;
  //void *send_region;

  //int num_completions;
};


static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);

static void post_receives(struct rdma_cm_id *id, int length);
static void post_receives_large(struct rdma_cm_id *id);
static void post_send(struct rdma_cm_id *id, RdmaMReq *req);
static void register_memory(struct client_context *c_context,size_t lengthSend,size_t lengthRecv);
static void deregister_memory(struct client_context *c_context);

static int on_addr_resolved(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static int on_route_resolved(struct rdma_cm_id *id);
static void build_params(struct rdma_conn_param *params);
void * poll_cq(void *ctx);
//void poll_cq(void *ctx, enum ibv_wc_opcode status);
//int poll_send(void *ctx);
//int poll_recv(void *ctx, void *rdmamres, void *buf, int headlength,int contentlength);
int get_comm_num(MPI_Comm comm, int *num);
int ReadOneMessage();


static struct context *s_ctx = NULL;

static struct rdma_cm_event *event = NULL;
static struct rdma_cm_id *rdcmid = NULL;
//struct connection *g_conn = NULL;
static struct rdma_event_channel *ec = NULL;
size_t reqsize = sizeof(RdmaMReq);
size_t ressize = sizeof(RdmaMRes);
//int reswssize = sizeof(RdmaMResWs);
//int reswossize = sizeof(RdmaMResWos);

int IfFileOpen = 0;
//static int IfSelected = 0;
//FILE *file;
FILE *listfile;
FILE *addrfile;
FILE *file;
FILE *commfile;
int ProcessID;
int PortID;
int comm_size;
MyHashMap *SendTable;
MyHashMap *RecvTable;
MyHashMap *IrecvTable;
MyHashMap *IsendTable;
MyHashMap *WaitTable;
MyHashMap *ReqWaitTable;
MyHashMap *commsizeTable;
MyList *ReduceList;
MyList *AllreduceList;
MyList *AlltoallList;
MyList *AlltoallvList;
MyList *BcastList;
//int send_time = 0;
//int recv_time = 0;
int sleepus = 50;
int request_static = 1;
int ProcessOnSameNode[16];
int shmid_msg;
int shmid_data;
int msgid;
int dataid;
struct shmmessage *shmmsg;
void *buf_tmp;
float displaystarttime;
float displaystoptime;


void mpi_comm_size_(int *comm, int *size, int *ierr)
{

	//printf("enter mpi_comm_size\n");
        int * size_tmp = (int *)malloc(sizeof(int));
        fread(size_tmp,sizeof(int),1,commfile);
        *size = *size_tmp;
        MPI_Comm *comm_tmp = (MPI_Comm *)malloc(sizeof(MPI_Comm));
        *comm_tmp = (MPI_Comm)(*comm);
        myHashMapPutData(commsizeTable, comm_tmp, size_tmp);
        //printf("mpi_comm_size: comm is %d, size is %d\n", *comm,  *size);
}

void mpi_comm_rank_(int *comm, int *rank, int *ierr)
{
	//printf("enter mpi_comm_rank\n");
    	int rank_tmp;
        fread(&rank_tmp,sizeof(int),1,commfile);
        *rank = rank_tmp;
        //printf("mpi_comm_rank, comm is %d, rank is %d\n", *comm, *rank);
}

void mpi_comm_dup_(int *comm, int *newcomm, int *ierr)
{
	
	//printf("enter mpi_comm_dup\n");
    	int comm_tmp;
        fread(&comm_tmp,sizeof(int),1,commfile);
	
        *newcomm = comm_tmp;
	
	
        int * size_tmp = (int *)malloc(sizeof(int));
	get_comm_num(comm_tmp, size_tmp);
	MPI_Comm *newcomm_tmp = (MPI_Comm *)malloc(sizeof(MPI_Comm));
	*newcomm_tmp = (MPI_Comm)(*newcomm);
        myHashMapPutData(commsizeTable, newcomm_tmp, size_tmp);
        
	//printf("mpi_comm_dup, comm is %d, rank is %d\n", *comm, *newcomm);
}




void mpi_comm_split_ ( int *comm, int *color, int *key, int *newcomm, int *ierr )
{
	//printf("enter mpi_comm_split\n");
    	int comm_tmp;
        fread(&comm_tmp,sizeof(int),1,commfile);
        *newcomm = comm_tmp;
        
	int * size_tmp = (int *)malloc(sizeof(int));
	get_comm_num(*comm, size_tmp);
	*size_tmp = (*size_tmp) / 2;
	MPI_Comm *newcomm_tmp = (MPI_Comm *)malloc(sizeof(MPI_Comm));
	*newcomm_tmp = (MPI_Comm)(*newcomm);
        myHashMapPutData(commsizeTable, newcomm_tmp, size_tmp);
        //printf("mpi_comm_split, comm is %d, rank is %d\n", *comm, *newcomm);
}



void mpi_init_(int *ierr)
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
	for(int flag =0; flag<16; flag++)
        {
                ProcessOnSameNode[flag] = flag;
        }
	char address[20];
	char port[10];
	struct addrinfo *addr;
	if((listfile=fopen("ProcessList","r"))==NULL) 
	{
		return;
	}
	/*if((addrfile=fopen("Addr_Port_File","r"))==NULL) 
	{
		return 1;
	}*/
	*ierr = MPI_Init(0,0);
	
	char tempList[20];
	if(fgets(tempList,128,listfile)!=NULL)
	{
		comm_size = char2int(tempList);
	}

	int id;
	MPI_Comm_rank(MPI_COMM_WORLD,&id);
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
                return;
        }
	//char* address_tmp = address;
        //char* port_tmp = port;
	////printf("address_tmp is %s\n", address_tmp);
	////printf("port_tmp is %s\n", port_tmp);
	fclose(listfile);
	
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
       int vl = getaddrinfo(address,port, NULL, &addr);
       ////printf("returnvalue is %d\n",vl);
       if(vl!=0)
       {
	       exit(EXIT_FAILURE);
       }

        TEST_Z(ec = rdma_create_event_channel());
        TEST_NZ(rdma_create_id(ec, &rdcmid, NULL, RDMA_PS_TCP));
        TEST_NZ(rdma_resolve_addr(rdcmid, NULL, addr->ai_addr, TIMEOUT_IN_MS));    
        freeaddrinfo(addr);  

  	int num_completions = 0;
	while (rdma_get_cm_event(ec, &event) == 0) {
    		struct rdma_cm_event event_copy;

    		memcpy(&event_copy, event, sizeof(*event));
    		rdma_ack_cm_event(event);

    		if (on_event(&event_copy))
      			break;
		if (++num_completions == 3)
			break;
  	}   

        msgid = 50 + ProcessID;
        dataid = 60 + ProcessID;
        //printf("msgid is %d\n", msgid);
        shmid_msg = get_shmid(msgid);
        shmmsg = (struct shmmessage*)shmat(shmid_msg,NULL,0);

        printf("shmid_msg is %d\n", shmid_msg);
        shmid_data = create_shm(BUFFER_SIZE, dataid);
        printf("shmid_data is %d\n",shmid_data);
        buf_tmp = shmat(shmid_data,NULL,0);

	SendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	RecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	WaitTable = createMyHashMap(myHashCodeRequest,myEqualRequest);
	//ReqWaitTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IrecvTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
	IsendTable = createMyHashMap(myHashCodeMSGL,myEqualMSGL);
        commsizeTable = createMyHashMap(myHashCodeMPIComm, myEqualInt);
 	ReduceList = createMyList();
	AllreduceList = createMyList();
	AlltoallList = createMyList();

	AlltoallvList = createMyList();
	BcastList = createMyList();
        
 	while(Message_Arrive == 0)
		usleep(sleepus);
	////printf("%d : leave init\n",ProcessID);

}

void mpi_send_(void *buf,int *count, int *datatype, int *dest, int *tag, int *comm, int *ierr)
{
	//printf("%d : enter Send\n",ProcessID);
	//printf("%d : enter Send,dest is %d, tag is %d, type is %d, comm is %d\n",ProcessID, *dest,*tag,*datatype,*comm);
	if(IfSameNode(*dest,ProcessOnSameNode,nodesize) == 0)
	{
		shmmsg->shmmreq.MsgType = ReadMsg;
		shmmsg->response = 0;
		shmmsg->request = 1;
                while(shmmsg->response == 0)
                        usleep(sleepus);
		MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
		msgl->source = *dest;
		msgl->tag = *tag;
		msgl->comm = (MPI_Comm)(*comm);
		msgl->datatype = (MPI_Datatype)(*datatype);

		RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
		int *returnvalue = (int *)malloc(sizeof(int));
		*returnvalue = 100;
		rbwos->returnvalue = returnvalue;
		rbwos->buf = NULL;
		//rbwos->totalsize = 0;
		myHashMapPutData(SendTable, msgl, rbwos);


		
		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
		rdmamreq->MsgType = Send;
		rdmamreq->datatype = (MPI_Datatype)(*datatype);
		rdmamreq->source = *dest;
		rdmamreq->tag = *tag;
		rdmamreq->comm = (MPI_Comm)(*comm);
		//int length = *count * getsize((MPI_Datatype)(*datatype));
		////printf("Send: sendlength is %d\n",length);

		post_receives(rdcmid,ressize);
		//post_receives(rdcmid,BUFFER_SIZE);
		Message_Arrive = 0;
		post_send(rdcmid,rdmamreq);

				
		while(Message_Arrive == 0);
			usleep(sleepus);
		*ierr = *returnvalue;         	
		myHashMapRemoveDataByKey(SendTable,msgl);
		free(returnvalue);
		free(msgl);
		free(rdmamreq);
	}
	else
	{
		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
                rdmamreq->MsgType = ReadMsg;
                post_send(rdcmid,rdmamreq);
		free(rdmamreq);
                shmmsg->shmmreq.MsgType = Send;
                shmmsg->shmmreq.datatype = (MPI_Datatype)(*datatype);
                shmmsg->shmmreq.source = *dest;
                shmmsg->shmmreq.tag = *tag;
                shmmsg->shmmreq.comm = (MPI_Comm)(*comm);
                shmmsg->response = 0;
                shmmsg->request = 1;
		//printf("%d : set 1\n",ProcessID);

                while(shmmsg->response == 0)
                        usleep(sleepus);
                *ierr = shmmsg->shmmres.returnvalue;
	}
	//printf("%d : leave Send,returnvalue is %d\n",ProcessID,*ierr);

}

void mpi_isend_(void *buf, int *count, int *datatype, int *dest, int *tag, int *comm, int *request, int *ierr)
{
	//printf("%d :enter Isend\n", ProcessID);
	*request = request_static++;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *((MPI_Request *)request);
	//printf("Isend request is %d\n", *request_tmp);
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->MsgType = Isend;
	msgl->source = *dest;
	msgl->tag = *tag;
	msgl->comm = (MPI_Comm)(*comm);
	msgl->datatype = (MPI_Comm)(*datatype);
 	if(IfSameNode(*dest,ProcessOnSameNode,nodesize) == 0)
	{
		int *returnvalue = (int *)malloc(sizeof(int));
		*returnvalue = 2018;
        	MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
		RBWS *rbws = (RBWS *)malloc(sizeof(RBWS));
		rbws->returnvalue = returnvalue;
		rbws->buf = NULL;
		rbws->status = status;
		rbws->totalsize = 0;
		//MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
		//rbws->status = status;
		myHashMapPutData(IsendTable,msgl,rbws);
		myHashMapPutData(WaitTable,request_tmp,msgl);
	}
	else
	{
		MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
                msgcws->totalsize = 0;
                myHashMapPutData(IsendTable,msgl,msgcws);
                myHashMapPutData(WaitTable,request_tmp,msgl);

	}
	//myHashMapRemoveDataByKey(IrecvTable,msgl);
	//free(msgl);
	//printf("%d :leave isend, tag is %d, source is %d, datatype is %d, count is %d\n", ProcessID, *tag, *dest, *datatype, *count);

	*ierr = 0;
	////printf("%d :leave Irecv\n", ProcessID);

}


void mpi_recv_(void *buf,int *count,int *datatype,int *source,int *tag,int *comm,int *status, int *ierr)
{
	//printf("%d : enter Recv\n",ProcessID);

	////printf("%d : enter Recv:source is %d, tag is %d, comm is %d ,datatype is %d,count is %d\n",ProcessID,*source,*tag,*comm,*datatype,*count);

	int type_size;
        MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int length = *count * type_size;
	//int sleeptime = length / BLOCK_SIZE + 1;
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->source = *source;
	msgl->tag = *tag;
	msgl->comm = (MPI_Comm)(*comm);
	msgl->datatype = (MPI_Datatype)(*datatype);
	if(IfSameNode(*source,ProcessOnSameNode,nodesize) == 0)
	{
		shmmsg->shmmreq.MsgType = ReadMsg;
		shmmsg->response = 0;
		shmmsg->request = 1;
                while(shmmsg->response == 0)
                        usleep(sleepus);
		RBWS *rbws = (RBWS *)malloc(sizeof(RBWS));
		int *returnvalue = (int *)malloc(sizeof(int));
		rbws->returnvalue = returnvalue;
		rbws->buf = buf;
		rbws->status = (MPI_Status *)status;
		//rbws->totalsize = length;
		myHashMapPutData(RecvTable, msgl, rbws);

		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
		rdmamreq->MsgType = Recv;
		rdmamreq->datatype = (MPI_Datatype)(*datatype);
		rdmamreq->source = *source;
		rdmamreq->tag = *tag;
		rdmamreq->comm = (MPI_Datatype)(*comm);
		
		
		int head = 0;
		while(length > 0)
		{
			//post_receives(g_conn,ressize + BLOCK_SIZE);
			if(length>=BLOCK_SIZE)
				post_receives(rdcmid,BUFFER_SIZE);
			else
				post_receives(rdcmid,ressize + length);
				//post_receives(rdcmid,BUFFER_SIZE);
				
			rdmamreq->head = head;
			////printf("%d : recv,head is %d\n",ProcessID,rdmamreq->head);
		
			Message_Arrive = 0;
			
			post_send(rdcmid,rdmamreq);
			while(Message_Arrive == 0)
				usleep(sleepus);
			length = length - BLOCK_SIZE;
			head = head + BLOCK_SIZE;
		}


		*ierr = *returnvalue;         	
		myHashMapRemoveDataByKey(RecvTable,msgl);
		//free(msgl);
		free(returnvalue);
		free(rdmamreq);
	}
	else
	{	
		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
                rdmamreq->MsgType = ReadMsg;
                post_send(rdcmid,rdmamreq);
		free(rdmamreq);
                shmmsg->shmmreq.MsgType = Recv;
                shmmsg->shmmreq.datatype = (MPI_Datatype)(*datatype);
                shmmsg->shmmreq.source = *source;
                shmmsg->shmmreq.tag = *tag;
                shmmsg->shmmreq.comm = (MPI_Comm)(*comm);
                int head = 0;
                while(length > 0)
                {
                        shmmsg->shmmreq.head = head;
                        shmmsg->response = 0;
                        shmmsg->request = 1;
                        while(shmmsg->response == 0)
                                usleep(sleepus);
                        memcpy(buf+head, buf_tmp, shmmsg->shmmres.totalsize);
                        length = length - BUFFER_SIZE;
                        head = head + BUFFER_SIZE;
                }

                *ierr = shmmsg->shmmres.returnvalue;
                *((MPI_Status*)status) = shmmsg->shmmres.status;
	}
	
	//printf("%d : leave recv\n",ProcessID);

}

void mpi_irecv_(void * buf, int *count, int *datatype, int *source, int *tag, int *comm, int *request, int *ierr)
{
	
	//printf("%d :enter Irecv\n", ProcessID);
	*request = request_static++;
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *((MPI_Request *)request);
	//printf("Irecv: request is %d\n", *request_tmp);
	//int length = *count * getsize((MPI_Datatype)(*datatype));
	int type_size;
        MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int length = *count * type_size;
	MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
	msgl->MsgType = Irecv;
	msgl->source = *source;
	msgl->tag = *tag;
	msgl->comm = (MPI_Comm)(*comm);
	msgl->datatype = (MPI_Datatype)(*datatype);
	if(IfSameNode(*source,ProcessOnSameNode,nodesize) == 0)
	{
		int *returnvalue = (int *)malloc(sizeof(int));
		*returnvalue = 2018;
		MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
		RBWS *rbws = (RBWS *)malloc(sizeof(RBWS));
		rbws->returnvalue = returnvalue;
		rbws->buf = buf;
		rbws->status = status;
		rbws->totalsize = length;
		//MPI_Status *status = (MPI_Status *)malloc(sizeof(MPI_Status));
		//rbws->status = status;
		myHashMapPutData(IrecvTable,msgl,rbws);
		myHashMapPutData(WaitTable,request_tmp,msgl);
	}
	else
	{
		MSGCWS *msgcws = (MSGCWS *)malloc(sizeof(MSGCWS));
                msgcws->buf = buf;
                msgcws->totalsize = length;
                myHashMapPutData(WaitTable,request_tmp,msgl);
                myHashMapPutData(IrecvTable,msgl,msgcws);
	
	}	
	//myHashMapRemoveDataByKey(IrecvTable,msgl);
	//free(msgl);
	//printf("%d :leave Irecv, length is %d. tag is %d, source is %d, datatype is %d, count is %d\n, comm is %d", ProcessID, length, *tag, *source, *datatype, *count, *comm);

	*ierr = 0;
	////printf("%d :leave Irecv\n", ProcessID);

}

void mpi_wait_(int *request, int *status, int *ierr)
{	
	//printf("%d: enter wait\n",ProcessID);
	MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
	*request_tmp = *((MPI_Request *)request);
	////printf("request is %d\n", *request_tmp);
  	MSGL *msgl = (MSGL *)myHashMapGetDataByKey(WaitTable,request_tmp);
	if(IfSameNode(msgl->source,ProcessOnSameNode,nodesize) == 0)
	{
		
		shmmsg->shmmreq.MsgType = ReadMsg;
                shmmsg->response = 0;
                shmmsg->request = 1;
                while(shmmsg->response == 0)
                        usleep(sleepus);
		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
		rdmamreq->datatype = msgl->datatype;
		rdmamreq->source = msgl->source;
		rdmamreq->tag = msgl->tag;
		rdmamreq->comm = msgl->comm;
		RBWS *rbws;
		if(msgl->MsgType == Isend)
		{
			rdmamreq->MsgType = Isend;
			rbws = (RBWS *)myHashMapGetDataByKey(IsendTable,msgl);
			post_receives(rdcmid,ressize);
			//post_receives(rdcmid,BUFFER_SIZE);
			Message_Arrive = 0;
			post_send(rdcmid,rdmamreq);

				
			while(Message_Arrive == 0);
				usleep(sleepus);
			*ierr = *(rbws->returnvalue);
			*((MPI_Status *)status) = *(rbws->status);

			free(rbws->status);
			free(rbws->returnvalue);

			myHashMapRemoveDataByKey(IsendTable,msgl);
			
		}
		else
		{
			rdmamreq->MsgType = Irecv;
			rbws = (RBWS *)myHashMapGetDataByKey(IrecvTable,msgl);		
			int length = rbws->totalsize;
		
			////printf("wait length is %d\n", length);
			int head = 0;
			while(length > 0)
			{
				//post_receives(g_conn,ressize + BLOCK_SIZE);
				if(length>=BLOCK_SIZE)
					post_receives(rdcmid,BUFFER_SIZE);
				else
					post_receives(rdcmid,ressize + length);
				//post_receives(rdcmid,BUFFER_SIZE);
				
				rdmamreq->head = head;
		
				Message_Arrive = 0;
			
				post_send(rdcmid,rdmamreq);
				while(Message_Arrive == 0)
					usleep(sleepus);
				length = length - BLOCK_SIZE;
				head = head + BLOCK_SIZE;
			}
			*ierr = *(rbws->returnvalue);
			*((MPI_Status *)status) = *(rbws->status);

			free(rbws->status);
			free(rbws->returnvalue);

			myHashMapRemoveDataByKey(IrecvTable,msgl);
		}
		
		myHashMapRemoveDataByKey(WaitTable,request_tmp);
		free(request_tmp);
	}
	else
	{
		RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
                rdmamreq->MsgType = ReadMsg;
                post_send(rdcmid,rdmamreq);
		free(rdmamreq);
                if(msgl->MsgType == Isend)
                {
                        shmmsg->shmmreq.MsgType = Isend;
                        shmmsg->shmmreq.datatype = msgl->datatype;
                        shmmsg->shmmreq.source = msgl->source;
                        shmmsg->shmmreq.tag = msgl->tag;
                        shmmsg->shmmreq.comm = msgl->comm;

                        shmmsg->response = 0;
                        shmmsg->request = 1;

                        while(shmmsg->response == 0)
                          usleep(sleepus);

                        myHashMapRemoveDataByKey(IsendTable, msgl);
                }
		
                else
                {
                        shmmsg->shmmreq.MsgType = Irecv;
                        shmmsg->shmmreq.datatype = msgl->datatype;
                        shmmsg->shmmreq.source = msgl->source;
                        shmmsg->shmmreq.tag = msgl->tag;
                        shmmsg->shmmreq.comm = msgl->comm;
                        MSGCWS *msgcws = myHashMapGetDataByKey(IrecvTable,msgl);
                        int length = msgcws->totalsize;
                        int head = 0;
                        while(length > 0)
                        {
                                shmmsg->shmmreq.head = head;
                                shmmsg->response = 0;
                                shmmsg->request = 1;
                                while(shmmsg->response == 0)
                                        usleep(sleepus);
                                memcpy(msgcws->buf+head, buf_tmp, shmmsg->shmmres.totalsize);
                                length = length - BUFFER_SIZE;
                                head = head + BUFFER_SIZE;
                        }

                        myHashMapRemoveDataByKey(IrecvTable,msgl);

                }

                *ierr = shmmsg->shmmres.returnvalue;
                *((MPI_Status*)status) = shmmsg->shmmres.status;
	}
	//printf("%d: leave wait return is %d\n",ProcessID, *ierr);
	 
}

void mpi_waitall_(int *count, int *array_of_requests, int *array_of_statuses, int *ierr)
{
	 //printf("%d: enter waitall\n",ProcessID);
	 for(int i=0; i<(*count); i++)
	 {
		MPI_Request *request_tmp = (MPI_Request *)malloc(sizeof(MPI_Request));
                *request_tmp = *((MPI_Request *)(array_of_requests + i));
		//printf("Waitall: request is %d\n", *request_tmp);
  		MSGL *msgl = (MSGL *)myHashMapGetDataByKey(WaitTable,request_tmp);
		
                if(IfSameNode(msgl->source,ProcessOnSameNode,comm_size) == 0)
		{
			shmmsg->shmmreq.MsgType = ReadMsg;
                	shmmsg->response = 0;
                	shmmsg->request = 1;
                	while(shmmsg->response == 0)
                        	usleep(sleepus);

			RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
			rdmamreq->datatype = msgl->datatype;
			rdmamreq->source = msgl->source;
			rdmamreq->tag = msgl->tag;
			rdmamreq->comm = msgl->comm;
			RBWS *rbws;
			if(msgl->MsgType == Isend)
			{
				//printf("Waitall: enter Isend\n");
				rdmamreq->MsgType = Isend;
				rbws = (RBWS *)myHashMapGetDataByKey(IsendTable,msgl);
				post_receives(rdcmid,ressize);
				//post_receives(rdcmid,BUFFER_SIZE);
				Message_Arrive = 0;
				post_send(rdcmid,rdmamreq);

				
				//printf("Waitall: Isend have send to messageserver\n");
				while(Message_Arrive == 0);
					usleep(sleepus);
				*ierr = *(rbws->returnvalue);
				*((MPI_Status *)(array_of_statuses + i)) = *(rbws->status);

				free(rbws->status);
				free(rbws->returnvalue);

				myHashMapRemoveDataByKey(IsendTable,msgl);
			
			}
			else
			{
				//printf("Waitall: enter Irecv\n");
				rdmamreq->MsgType = Irecv;
				//printf("waitall Irecv: tag is %d, source is %d, datatype is %d, comm is %d", msgl->tag, msgl->source, msgl->datatype, msgl->comm);
				rbws = (RBWS *)myHashMapGetDataByKey(IrecvTable,msgl);		
				int length = rbws->totalsize;
				//printf("wait length is %d\n", length);
				int head = 0;
				while(length > 0)
				{
					//post_receives(g_conn,ressize + BLOCK_SIZE);
					if(length>=BLOCK_SIZE)
						post_receives(rdcmid,BUFFER_SIZE);
					else
						post_receives(rdcmid,ressize + length);
					//post_receives(rdcmid,BUFFER_SIZE);
				
					rdmamreq->head = head;
		
					Message_Arrive = 0;
			
					post_send(rdcmid,rdmamreq);
					while(Message_Arrive == 0)
						usleep(sleepus);
					length = length - BLOCK_SIZE;
					head = head + BLOCK_SIZE;
				}
				*ierr = *(rbws->returnvalue);
				*((MPI_Status *)(array_of_statuses + i)) = *(rbws->status);

				free(rbws->status);
				free(rbws->returnvalue);
				myHashMapRemoveDataByKey(IrecvTable,msgl);
			}

			

			myHashMapRemoveDataByKey(WaitTable,request_tmp);
			free(request_tmp);
		}
		else
		{
			RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
                        rdmamreq->MsgType = ReadMsg;
                        post_send(rdcmid,rdmamreq);
			free(rdmamreq);
                        if(msgl->MsgType == Isend)
                        {
                                shmmsg->shmmreq.MsgType = Isend;
                                shmmsg->shmmreq.datatype = msgl->datatype;
                                shmmsg->shmmreq.source = msgl->source;
                                shmmsg->shmmreq.tag = msgl->tag;
                                shmmsg->shmmreq.comm = msgl->comm;

                                shmmsg->response = 0;
                                shmmsg->request = 1;

                                while(shmmsg->response == 0)
                                  usleep(sleepus);

                                myHashMapRemoveDataByKey(IsendTable, msgl);
                        }
			else
                        {
                                shmmsg->shmmreq.MsgType = Irecv;
                                shmmsg->shmmreq.datatype = msgl->datatype;
                                shmmsg->shmmreq.source = msgl->source;
                                shmmsg->shmmreq.tag = msgl->tag;
                                shmmsg->shmmreq.comm = msgl->comm;
                                MSGCWS *msgcws = myHashMapGetDataByKey(IrecvTable,msgl);
                                int length = msgcws->totalsize;
                                int head = 0;
                                while(length > 0)
                                {
                                        shmmsg->shmmreq.head = head;
                                        shmmsg->response = 0;
                                        shmmsg->request = 1;
                                        while(shmmsg->response == 0)
                                                usleep(sleepus);
                                        memcpy(msgcws->buf+head, buf_tmp, shmmsg->shmmres.totalsize);
                                        length = length - BUFFER_SIZE;
                                        head = head + BUFFER_SIZE;
                                }
                                RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
                                rdmamreq->MsgType = ReadMsg;
                                post_send(rdcmid,rdmamreq);
                                         
                                myHashMapRemoveDataByKey(IrecvTable,msgl);
                                
                        }       

                        *ierr = shmmsg->shmmres.returnvalue;
                	*((MPI_Status*)(array_of_statuses + i)) = shmmsg->shmmres.status;

		}	
	 }
	 //printf("%d: leave waitall\n",ProcessID);
}

void mpi_alltoall_ (void* sendbuf, int *sendcount, int *sendtype, void* recvbuf, int *recvcount, int *recvtype, int *comm, int *ierr)
{
	//printf("%d: enter Alltoall\n",ProcessID);
    	shmmsg->shmmreq.MsgType = ReadMsg;
        shmmsg->response = 0;
        shmmsg->request = 1;
        while(shmmsg->response == 0)
             usleep(sleepus);
	int procNum; 
	get_comm_num((MPI_Comm)(*comm), &procNum);
	//int sendlength = procNum * (*sendcount) * getsize((MPI_Datatype)(*sendtype));
	//int recvlength = procNum * (*recvcount) * getsize((MPI_Datatype)(*recvtype));
	int send_type_size;
	int recv_type_size;
        MPI_Type_size((MPI_Datatype)(*sendtype), &send_type_size);
        MPI_Type_size((MPI_Datatype)(*recvtype), &recv_type_size);
	int sendlength = procNum * (*sendcount) * send_type_size;
	int recvlength = procNum * (*recvcount) * recv_type_size;
	//int sendlength = procNum * 256 * send_type_size;
	//int recvlength = procNum * 256 * recv_type_size;
	//int sleeptime = recvlength / BLOCK_SIZE + 1;
	//printf("%d: Alltoall recvlength is %d, recvcount is %d, sendcount is %d, recv_type_size is %d\n",ProcessID,recvlength,*recvcount,*sendcount,recv_type_size);
	RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
	int *returnvalue = (int *)malloc(sizeof(int));
	rbwos->returnvalue = returnvalue;
	rbwos->buf = recvbuf;
	//rbwos->totalsize = recvlength;
	myListInsertDataAtLast(AlltoallList,rbwos);
	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->MsgType = Alltoall;
	rdmamreq->datatype = (MPI_Datatype)(*recvtype);
	rdmamreq->source = 0;
	rdmamreq->tag = 1000;
	rdmamreq->comm = (MPI_Comm)(*comm);

	int head = 0;
	while(recvlength > 0)
	{
        	//post_receives(g_conn,ressize + BLOCK_SIZE);
		//printf("head is %d\n", head);
		if(recvlength>=BLOCK_SIZE)
        		post_receives(rdcmid,BUFFER_SIZE);
		else
        		post_receives(rdcmid,recvlength + ressize);
		rdmamreq->head = head;
	
 		Message_Arrive = 0;
                
		post_send(rdcmid,rdmamreq);
 		while(Message_Arrive == 0)
			usleep(sleepus);
		recvlength = recvlength - BLOCK_SIZE;
		head = head + BLOCK_SIZE;
	}
	
	//printf("%d: alltoall out while, Message_arrive is %d\n",ProcessID,Message_Arrive);
	
	*ierr = *returnvalue;         	

	myListRemoveDataAtFirst(AlltoallList);
	free(rdmamreq);
	//printf("%d: alltoall returnvalue is %d\n",ProcessID,*ierr);
}

void mpi_alltoallv_(void* sendbuf, int *sendcounts, int *sdispls, int *sendtype, void* recvbuf, int *recvcounts, int *rdispls, int *recvtype, int *comm, int *ierr)
{	

	//printf("%d: enter Alltoallv\n",ProcessID);
    	shmmsg->shmmreq.MsgType = ReadMsg;
        shmmsg->response = 0;
        shmmsg->request = 1;
        while(shmmsg->response == 0)
             usleep(sleepus);
	int procNum; 
	get_comm_num((MPI_Comm)(*comm), &procNum);
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
        MPI_Type_size((MPI_Datatype)(*sendtype), &send_type_size);
        MPI_Type_size((MPI_Datatype)(*recvtype), &recv_type_size);
	int sendlength = sendcount * send_type_size;
	int recvlength = recvcount * recv_type_size;
	//int sleeptime = recvlength / BLOCK_SIZE + 1;
	////printf("%d: Alltoallv sendcount is %d,recvcount is %d\n",ProcessID,sendcount,recvcount);
	////printf("%d: Alltoallv sendlength is %d,recvlength is %d\n",ProcessID,sendlength,recvlength);
	RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
	int *returnvalue = (int *)malloc(sizeof(int));
	*returnvalue = 100;
	rbwos->returnvalue = returnvalue;
	rbwos->buf = recvbuf;
	//rbwos->totalsize = recvlength;
	myListInsertDataAtLast(AlltoallvList,rbwos);

	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->MsgType = Alltoallv;
	rdmamreq->datatype = (MPI_Datatype)(*recvtype);
	rdmamreq->source = 0;
	rdmamreq->tag = 1000;
	rdmamreq->comm = (MPI_Comm)(*comm);
        int head = 0;

	
        /*post_receives(rdcmid);
	recvlength = recvlength - BLOCK_SIZE;
	post_send(rdcmid,rdmamreq);*/
	while(recvlength > 0)
	{
        	//post_receives(g_conn,ressize + BLOCK_SIZE);
		if(recvlength>=BLOCK_SIZE)
        		post_receives(rdcmid,BUFFER_SIZE);
		else
        		post_receives(rdcmid,recvlength + ressize);
		rdmamreq->head = head;
	
 		Message_Arrive = 0;
                
		post_send(rdcmid,rdmamreq);
 		while(Message_Arrive == 0)
			usleep(sleepus);
		recvlength = recvlength - BLOCK_SIZE;
		head = head + BLOCK_SIZE;
	}
	
	
	*ierr = *returnvalue;         	
	myListRemoveDataAtFirst(AlltoallvList);
	free(rdmamreq);
	////printf("%d: alltoallv returnvalue is %d\n",ProcessID,r);
	//PMPI_Barrier(MPI_COMM_WORLD);
	////printf("%d: leave  alltoallv\n",ProcessID);
}


void mpi_allreduce_(void* sendbuf, void* recvbuf, int *count, int *datatype, int *op, int *comm, int *ierr)
{
	//deregister_memory(conn);
	//register_memory(conn,BUFFER_SIZE,BUFFER_SIZE);
	//printf("%d: enter Allreduce\n",ProcessID);
	//int sendlength = (*count) * getsize((MPI_Datatype)(*datatype));
	//int recvlength = (*count) * getsize((MPI_Datatype)(*datatype));
    	shmmsg->shmmreq.MsgType = ReadMsg;
        shmmsg->response = 0;
        shmmsg->request = 1;
        while(shmmsg->response == 0)
             usleep(sleepus);
	int type_size;
        MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int sendlength = (*count) * type_size;
	int recvlength = sendlength;
	//int sleeptime = recvlength / BLOCK_SIZE + 1;
	////printf("%d: Allreduce recvlength is %d\n",ProcessID,recvlength);
	RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
	int *returnvalue = (int *)malloc(sizeof(int));
	*returnvalue = 100;
	rbwos->returnvalue = returnvalue;
	rbwos->buf = recvbuf;
	//rbwos->totalsize = recvlength;
	myListInsertDataAtLast(AllreduceList,rbwos);

	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->MsgType = Allreduce;
	rdmamreq->datatype = (MPI_Datatype)(*datatype);
	rdmamreq->source = 0;
	rdmamreq->tag = 1000;
	
	int head=0;
	while(recvlength > 0)
	{
        	//post_receives(g_conn,ressize + BLOCK_SIZE);
		if(recvlength>=BLOCK_SIZE)
        		post_receives(rdcmid,BUFFER_SIZE);
		else
        		post_receives(rdcmid,ressize + recvlength);
		rdmamreq->head = head;
	
 		Message_Arrive = 0;
                
		post_send(rdcmid,rdmamreq);
 		while(Message_Arrive == 0)
			usleep(sleepus);
		recvlength = recvlength - BLOCK_SIZE;
		head = head + BLOCK_SIZE;
	}
	
	
	/*while(sleeptime > 0)
	{
		usleep(sleepus);
		sleeptime--;
	}*/
 	while(Message_Arrive == 0)
		usleep(sleepus);

        *ierr = *returnvalue;
	//printf("%d: allreduce returnvalue is %d\n",ProcessID,*ierr);

	free(rdmamreq);
	myListRemoveDataAtFirst(AllreduceList);
}	

void mpi_reduce_(void* sendbuf, void* recvbuf, int *count, int *datatype, int *op, int *root, int *comm, int *ierr)
{	
	//printf("%d: enter reduce\n",ProcessID);
    	shmmsg->shmmreq.MsgType = ReadMsg;
        shmmsg->response = 0;
        shmmsg->request = 1;
        while(shmmsg->response == 0)
             usleep(sleepus);

	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->datatype = (MPI_Datatype)(*datatype);
	rdmamreq->source = 0;
	rdmamreq->tag = 1000;
	
	//int sendlength = (*count) * getsize((MPI_Datatype)(*datatype));
	int type_size;
        MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	
	RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
	int *returnvalue = (int *)malloc(sizeof(int));
	rbwos->returnvalue = returnvalue;
        //int sleeptime = 0;
	if(ProcessID == *root)
	{
		int recvlength = (*count) * type_size;		
		//sleeptime = recvlength / BLOCK_SIZE + 1;
		rbwos->buf = recvbuf;
		//rbwos->totalsize = recvlength;
		myListInsertDataAtLast(ReduceList,rbwos);
		rdmamreq->MsgType = Reduce;
	
		int head=0;
		while(recvlength > 0)
		{
        		//post_receives(g_conn,ressize + BLOCK_SIZE);
			if(recvlength>=BLOCK_SIZE)
        			post_receives(rdcmid,ressize);
			else
        			post_receives(rdcmid,ressize + recvlength);
			rdmamreq->head = head;
	
 			Message_Arrive = 0;
                
			post_send(rdcmid,rdmamreq);
 			while(Message_Arrive == 0)
				usleep(sleepus);
			recvlength = recvlength - BLOCK_SIZE;
			head = head + BLOCK_SIZE;
		}
        	
        	//post_receives(g_conn,ressize + recvlength);
        	//post_send(g_conn,rdmamreq,NULL,reqsize, 0);
	}
	else
	{
		rbwos->buf = NULL;
		//rbwos->totalsize = 0;
		myListInsertDataAtLast(ReduceList,rbwos);
		rdmamreq->MsgType = Reduce_NR;
        	post_receives(rdcmid,ressize);
        	Message_Arrive = 0;
		post_send(rdcmid,rdmamreq);
 		while(Message_Arrive == 0)
			usleep(sleepus);


	}
	
        *ierr = *returnvalue;
	////printf("reduce returnvalue is %d\n",r);
	myListRemoveDataAtFirst(ReduceList);
	free(rdmamreq);

}

void mpi_bcast_ (void* buffer, int *count, int *datatype, int *root, int *comm, int *ierr )
{
	//printf("%d: enter Bcast\n",ProcessID);
    	shmmsg->shmmreq.MsgType = ReadMsg;
        shmmsg->response = 0;
        shmmsg->request = 1;
        while(shmmsg->response == 0)
             usleep(sleepus);
	int procNum; 
	get_comm_num((MPI_Comm)(*comm), &procNum);
        int type_size;
	MPI_Type_size((MPI_Datatype)(*datatype), &type_size);
	int sendlength = (*count) * type_size;
	int recvlength = sendlength;
	//int sleeptime = recvlength / BLOCK_SIZE + 1;
	////printf("%d: Alltoallv sendcount is %d,recvcount is %d\n",ProcessID,sendcount,recvcount);
	////printf("%d: Alltoallv sendlength is %d,recvlength is %d\n",ProcessID,sendlength,recvlength);
	RBWoS *rbwos = (RBWoS *)malloc(sizeof(RBWoS));
	int *returnvalue = (int *)malloc(sizeof(int));
	*returnvalue = 100;
	rbwos->returnvalue = returnvalue;
	rbwos->buf = buffer;
	//rbwos->totalsize = recvlength;
	myListInsertDataAtLast(BcastList,rbwos);

	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->MsgType = Bcast;
	rdmamreq->datatype = (MPI_Datatype)(*datatype);
	rdmamreq->source = 50000;
	rdmamreq->tag = 50000;
	rdmamreq->comm = (MPI_Comm)(*comm);
        int head = 0;

	
	while(recvlength > 0)
	{
		if(recvlength>=BLOCK_SIZE)
        		post_receives(rdcmid,BUFFER_SIZE);
		else
        		post_receives(rdcmid,ressize + recvlength);
		rdmamreq->head = head;
	
 		Message_Arrive = 0;
                
		post_send(rdcmid,rdmamreq);
 		while(Message_Arrive == 0)
			usleep(sleepus);
		recvlength = recvlength - BLOCK_SIZE;
		head = head + BLOCK_SIZE;
	}

	*ierr = *returnvalue;         	
	myListRemoveDataAtFirst(BcastList);
	free(rdmamreq);
	////printf("%d: alltoallv returnvalue is %d\n",ProcessID,r);
	//PMPI_Barrier(MPI_COMM_WORLD);
	////printf("%d: leave  alltoallv\n",ProcessID);
	
}

void mpi_finalize(int *ierr)
{
	printf("%d: enter finalize\n",ProcessID);
	
	RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
	rdmamreq->MsgType = Stop;
	
        post_receives(rdcmid,ressize);
	Message_Arrive = 0;
	post_send(rdcmid,rdmamreq);
	
 	while(Message_Arrive == 0)
		usleep(sleepus);
	
	//printf("%d: begin disconnect\n",ProcessID);
 	freeMyHashMap(SendTable);
        freeMyHashMap(RecvTable);
        freeMyHashMap(IrecvTable);
	freeMyHashMap(WaitTable);
        freeMyList(ReduceList);
        freeMyList(AllreduceList);
        freeMyList(AlltoallList);
        freeMyList(AlltoallvList);
        freeMyList(BcastList);
	rdma_disconnect(rdcmid);
	while (rdma_get_cm_event(ec, &event) == 0) {
    		struct rdma_cm_event event_copy;

    		memcpy(&event_copy, event, sizeof(*event));
    		rdma_ack_cm_event(event);

    		if (on_event(&event_copy))
      			break;
  	}   
	rdma_destroy_event_channel(ec);
	destroy_shm(shmid_data);
        destroy_shm(shmid_msg);
	*ierr = MPI_Finalize();
}



void register_memory(struct client_context *c_context, size_t lengthSend, size_t lengthRecv)
{
  //conn->send_region = malloc(lengthSend);
  posix_memalign((void **)&c_context->msg, sysconf(_SC_PAGESIZE), MESSAGE_SIZE);
  //conn->recv_region = malloc(lengthRecv);
  posix_memalign((void **)&c_context->recv_region, sysconf(_SC_PAGESIZE), BUFFER_SIZE);

  TEST_Z(c_context->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    c_context->msg, 
    MESSAGE_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(c_context->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    c_context->recv_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
  
}

void deregister_memory(struct client_context *c_context)
{
  
  ibv_dereg_mr(c_context->send_mr);
  ibv_dereg_mr(c_context->recv_mr);

  free(c_context->msg);
  free(c_context->recv_region);
  
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
  {
    r = on_addr_resolved(event->id);
    ////printf("addr_resolved\n");
  }
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
  {
    r = on_route_resolved(event->id);
    ////printf("route_resolved\n");
  }
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
  {
    r = on_connection(event->id);
    ////printf("event_established\n");
  }
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
  {
    r = on_disconnect(event->id);
    ////printf("event_disconnected\n");
  }
  else
    die("on_event: unknown event.");

  return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  //printf("route resolved.\n");
  build_params(&cm_params);
  //memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_connect(id, &cm_params));

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct client_context *c_context = (struct client_context *)id->context;

  //printf("disconnected.\n");

  rdma_destroy_qp(id);

  //ibv_dereg_mr(c_context->send_mr);
  //ibv_dereg_mr(c_context->recv_mr);

  //free(c_context->msg);
  //free(c_context->recv_region);
  deregister_memory(c_context);  

  free(c_context);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

int on_connection(struct rdma_cm_id *id)
{
  //printf("on_connection.\n");
  //g_conn = (struct connection *)context;
  
  //RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
  //rdmamreq->MsgType = Start;
  struct client_context *c_context = (struct client_context *)id->context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  c_context->msg->rdmamreq.MsgType = Start;
  c_context->msg->mr.addr = (uintptr_t)c_context->recv_mr->addr;
  c_context->msg->mr.rkey = c_context->recv_mr->rkey;
  //memcpy(&(c_context->msg->rdmamreq),rdmamreq,reqsize);
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)c_context->msg;
  sge.length = MESSAGE_SIZE;
  //sge.length = headlength + contentlength;;
  sge.lkey = c_context->send_mr->lkey;

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  
  ////printf("%d, first send.\n", ProcessID);
  return 0;
}

void on_completion(struct ibv_wc *wc)
{
  struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
  struct client_context *c_context = (struct client_context *)id->context;
  ////printf("status is %d,opcode is %d\n", wc->status,wc->opcode );

  if (wc->status != IBV_WC_SUCCESS)
  {
    //deregister_memory(c_context);
    //printf("status is %d,opcode is %d\n", wc->status,wc->opcode );
    //die("on_completion: status is not IBV_WC_SUCCESS.");
  }
  if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM)
  {
    ////printf("enter IBV_WC_RECV.%d \n",recv_time);
    ////printf("received message: %s\n", conn->recv_region);

    ////printf("received message: %s\n", conn->recv_region);
    ////printf("%d, status is %d,opcode is %d,imm_data is %d\n", ProcessID, wc->status,wc->opcode,wc->imm_data );
    RdmaMRes *rdmamres = (RdmaMRes *)malloc(ressize);
    memcpy(rdmamres,c_context->recv_region,ressize);   
    MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
    RBWoS *rbwos;
    RBWS *rbws;
    ////printf("respond type is %d.\n",rdmamres->MsgType);
    ////printf(" head is %d ,totalsize is %d\n",rdmamres->head,rdmamres->totalsize);
    ////printf("%d, respond type is %d\n",ProcessID,rdmamres->MsgType);
    switch(rdmamres->MsgType)
    {
	
	case Start:
    		//printf("%d,respond type is Start.\n", ProcessID);
		Message_Arrive = 1;
                
		break;
        case Send:
    		////printf("respond type is %d.\n",rdmamres->MsgType);
		////printf("%d : Sendresponse: dest is %d,tag is %d, comm is %d",ProcessID,rdmamres->source,rdmamres->tag,rdmamres->comm);
		msgl->source = rdmamres->source;
		msgl->tag = rdmamres->tag;
		msgl->comm = rdmamres->comm;
		msgl->datatype = rdmamres->datatype;
		rbwos = (RBWoS *)myHashMapGetDataByKey(SendTable,msgl);
		/*if(!rbwos) 
    			//printf("send is NULL\n");*/

		*(rbwos->returnvalue) = rdmamres->returnvalue;
		Message_Arrive = 1;
    		////printf("leave respond type is %d.\n",rdmamres->MsgType);
		//myHashMapRemoveDataByKey(SendTable,msgl);
		break;
        case Isend:
    		////printf("respond type is %d.\n",rdmamres->MsgType);
		////printf("%d : Sendresponse: dest is %d,tag is %d, comm is %d",ProcessID,rdmamres->source,rdmamres->tag,rdmamres->comm);
		msgl->source = rdmamres->source;
		msgl->tag = rdmamres->tag;
		msgl->comm = rdmamres->comm;
		msgl->datatype = rdmamres->datatype;
		rbws = (RBWS *)myHashMapGetDataByKey(IsendTable,msgl);
		/*if(!rbwos) 
    			//printf("send is NULL\n");*/

		*(rbws->returnvalue) = rdmamres->returnvalue;
		*(rbws->status) = rdmamres->status;
		Message_Arrive = 1;
    		////printf("leave respond type is %d.\n",rdmamres->MsgType);
		//myHashMapRemoveDataByKey(SendTable,msgl);
		break;
	case Recv:
		msgl->source = rdmamres->source;
		msgl->tag = rdmamres->tag;
		msgl->comm = rdmamres->comm;
		msgl->datatype = rdmamres->datatype;
		rbws = (RBWS *)myHashMapGetDataByKey(RecvTable,msgl);
		*(rbws->returnvalue) = rdmamres->returnvalue;
		*(rbws->status) = rdmamres->status;
	        memcpy(rbws->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		Message_Arrive = 1;
		break;
	case Irecv:
		//printf("poll :enter Irecv\n");
		msgl->source = rdmamres->source;
		msgl->tag = rdmamres->tag;
		msgl->comm = rdmamres->comm;
		msgl->datatype = rdmamres->datatype;
		//printf("poll :Irecv, tag is %d, source is %d, datatype is %d, comm is %d\n", msgl->tag, msgl->source, msgl->datatype, msgl->comm);
		rbws = (RBWS *)myHashMapGetDataByKey(IrecvTable,msgl);
		//if(rbws==NULL)  //printf("poll : null\n");
		memcpy(rbws->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		*(rbws->returnvalue) = rdmamres->returnvalue;
		*(rbws->status) = rdmamres->status;
		Message_Arrive = 1;
		//printf("poll :leave Irecv\n");
		break;
	case Reduce:
		rbwos = (RBWoS *)myListGetDataAtFirst(ReduceList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		memcpy(rbwos->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		Message_Arrive = 1;
		//myListRemoveDataAtFirst(ReduceList);
		break;
	case Reduce_NR:
		rbwos = (RBWoS *)myListGetDataAtFirst(ReduceList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		Message_Arrive = 1;
		
		break;
	case Allreduce:
    		////printf("respond type is Allreduce.\n");
		rbwos = (RBWoS *)myListGetDataAtFirst(AllreduceList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		memcpy(rbwos->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		Message_Arrive = 1;
    		////printf("allreduce head is %d ,totalsize is %d\n",rdmamres->head,rdmamres->totalsize);
		break;
	case Alltoall:
    		//printf("respond type is Alltoall, head is %d\n", rdmamres->head);
		//if(myListGetSize(AlltoallList) == 0) break;
		rbwos = (RBWoS *)myListGetDataAtFirst(AlltoallList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		memcpy(rbwos->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		Message_Arrive = 1;
    		//printf("alltoall head is %d ,totalsize is %d, returnvalue is %d\n",rdmamres->head,rdmamres->totalsize, rdmamres->returnvalue);
		break;
	case Alltoallv:
    		////printf("respond type is Alltoallv.\n");
		rbwos = (RBWoS *)myListGetDataAtFirst(AlltoallvList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		memcpy(rbwos->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		//if(rdmamres->totalsize < BUFFER_SIZE) 
		Message_Arrive = 1;
    		////printf("alltoallv head is %d ,totalsize is %d\n",rdmamres->head,rdmamres->totalsize);
		break;
	case Bcast:
		rbwos = (RBWoS *)myListGetDataAtFirst(BcastList);
		*rbwos->returnvalue = rdmamres->returnvalue;
		memcpy(rbwos->buf + rdmamres->head,c_context->recv_region + ressize, rdmamres->totalsize);
		Message_Arrive = 1;
		break;
	case Stop:
    		//printf("%d, respond type is Stop\n",ProcessID);
		Message_Arrive = 1;
		break;	
	default:
		break;
    }
    
    free(msgl);
    free(rdmamres);
    //usleep(100);
  }
  else if (wc->opcode == IBV_WC_SEND)
	  ;
    ////printf("send completed successfully,%d\n",send_time);
  else
    die("on_completion: completion isn't a send or a receive.");

  //if (++conn->num_completions == 2)
    //rdma_disconnect(conn->id);
}

int on_addr_resolved(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct client_context *c_context;

  ////printf("address resolved.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = c_context = (struct client_context *)malloc(sizeof(struct client_context));

  //conn->id = id;
  //conn->qp = id->qp;
  register_memory(c_context,BUFFER_SIZE,BUFFER_SIZE);
  //printf("first recv\n");
  //post_receives_large(id);
  post_receives(id, ressize);
  //printf("first recv success\n");

  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}

void post_send(struct rdma_cm_id *id,RdmaMReq *rdmamreq)
{
  ////printf("enter post_send , %d\n", ++send_time);
  struct client_context *c_context = (struct client_context *)id->context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;


  memcpy(&(c_context->msg->rdmamreq),rdmamreq,reqsize);
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)c_context->msg;
  sge.length = MESSAGE_SIZE;
  //sge.length = headlength + contentlength;;
  sge.lkey = c_context->send_mr->lkey;

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  ////printf("leave post_send,%d\n",send_time);
  
}

/*void post_receives(struct rdma_cm_id *id)
{
  ////printf("enter post_receives,%d\n",++recv_time);
  struct ibv_recv_wr wr, *bad_wr = NULL;
  // struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = (uintptr_t)id;
  wr.sg_list = NULL;
  wr.num_sge = 0;


  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  ////printf("leave post_receives,%d\n",recv_time);
}*/

void post_receives_large(struct rdma_cm_id *id)
{
  ////printf("enter post_receives,%d\n",++recv_time);
  struct client_context *c_context = (struct client_context *)id->context;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  
  sge.addr = (uintptr_t)c_context->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = c_context->recv_mr->lkey;


  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  ////printf("leave post_receives,%d\n",recv_time);

}

void post_receives(struct rdma_cm_id *id,int length)
{
  ////printf("enter post_receives,%d\n",++recv_time);
  struct client_context *c_context = (struct client_context *)id->context;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  
  sge.addr = (uintptr_t)c_context->recv_region;
  sge.length = length;
  sge.lkey = c_context->recv_mr->lkey;


  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  ////printf("leave post_receives,%d\n",recv_time);

}



/*int poll_send(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc *wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    //printf("in poll_cq\n");
    if (ibv_poll_cq(cq, 1, wc)){
      //on_completion(&wc);
	
  	if (wc->status != IBV_WC_SUCCESS)
		return 0;
  	if (wc->opcode & IBV_WC_SEND)
  		return 1;
	else
		return 0;
    }
  }
}*/

/*int poll_recv(void *ctx, RdmaMRes *rdmamres, void *buf, int headlength,int contentlength)
{
  struct ibv_cq *cq;
  struct ibv_wc *wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    //printf("in poll_cq\n");
    if (ibv_poll_cq(cq, 1, wc)){
      //on_completion(&wc);
	
  	if (wc->status != IBV_WC_SUCCESS)
		return 0;
  	if (wc->opcode & IBV_WC_RECV)
	{
  		struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
                memcpy(rdmamres,conn->recv_region,headlength);
		if(!contentlength)
			memcpy(buf,conn->recv_region + headlength,contentlength);
		return 1;
	}
	else
		return 0;
    }
  }
}*/

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

void die(const char *reason)
{
  //fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}


int get_comm_num(MPI_Comm comm, int *num)
{
	//printf("get size: comm is %d.\n", comm);

  	int *num_tmp = (int *)myHashMapGetDataByKey(commsizeTable,&comm);
  	*num = *num_tmp;
  	//printf("get size is %d.\n", *num);
  	return 0;
}

