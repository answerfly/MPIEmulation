//#include<sys/time.h>
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
#include <rdma/rdma_cma.h>

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

const size_t BUFFER_SIZE = 10 * 1024 * 1024;
const size_t BLOCK_SIZE = 10 * 1024 * 1024 - sizeof(RdmaMRes);
const size_t BUFFER_SIZE_SMALL = 8 * 1024;
const size_t BLOCK_SIZE_SMALL = 8 * 1024 - sizeof(RdmaMRes);
const size_t MESSAGE_SIZE = sizeof(struct message) ;

const int TIMEOUT_IN_MS = 500; /* ms */


struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct server_context {
  //struct ibv_qp *qp;
  
  struct message *msg;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  //void *recv_region;
  void *send_region;

  uint64_t peer_addr;
  uint32_t peer_rkey;

};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void build_params(struct rdma_conn_param *params);
static void * poll_cq(void *);
//static void post_receives(struct connection *conn);
//static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static void post_receives(struct rdma_cm_id *id);
//static int post_send(struct rdma_cm_id *id, void *res, void *buf, int headlength, int contentlength);
static void write_remote(struct rdma_cm_id *id, uint32_t len);
static void register_memory(struct server_context* s_context,int lengthSend,int lengthRecv);
static void deregister_memory(struct server_context *s_context);


static struct context *s_ctx = NULL;

struct rdma_cm_event *event = NULL;
struct rdma_cm_id *listener = NULL;
struct rdma_event_channel *ec = NULL;
//struct connection *conn = NULL;
const size_t reqsize = sizeof(RdmaMReq);
//int reswssize = sizeof(RdmaMResWs);
//int reswossize = sizeof(RdmaMResWos);
const size_t ressize = sizeof(RdmaMRes);
int communication_num = 0;
int communication_current = 1;
int expectedRecvNum[256];
//int int2char(int id,char *idString);
//int char2int(char *ProcessID_String);
int ReadOneMessage();
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
		//expectedRecvNum[++communication_num] = recv_num;
	        ////printf("recv_num is %d\n", recv_num);

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
	#if _USE_IPV6
  	struct sockaddr_in6 addr;
	#else
  	struct sockaddr_in addr;
	#endif
	  	
  	uint16_t port1 = 0;
	char port[5] = "50000";
	memcpy(port+5-strlen(argv[1]),argv[1],strlen(argv[1]));

	memset(&addr, 0, sizeof(addr));
	#if _USE_IPV6
	addr.sin6_family = AF_INET6;
	addr.sin6_port = htons(atoi(port));
	#else
	addr.sin_family = AF_INET;
	addr.sin_port = htons(atoi(port));
	#endif

	TEST_Z(ec = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
	TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

	port1 = ntohs(rdma_get_src_port(listener));

	printf("listening on port %d.\n", port1);

	while (rdma_get_cm_event(ec, &event) == 0) {
	  struct rdma_cm_event event_copy;

	  memcpy(&event_copy, event, sizeof(*event));
	  rdma_ack_cm_event(event);

	  if (on_event(&event_copy))
	    break;
	}

	rdma_destroy_id(listener);
	rdma_destroy_event_channel(ec);

	
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
	RMR *rmr = (RMR *)malloc(sizeof(RMR));
	if(fread(rmr,sizeof(RMR),1,file)==0)
	{
		free(rmr);
		return 0;
	}
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
			free(buf);
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
	RMR *rmr = (RMR *)malloc(sizeof(RMR));
	if(fread(rmr,sizeof(RMR),1,file)==0)
	{
		free(rmr);
		return 0;
	}
	void *buf = malloc(rmr->typesize * rmr->recvcount);
	switch(rmr->MsgType)
	{
		case Send:
		 	break;
		case Recv:			
			fread(buf,rmr->typesize,rmr->recvcount,file);	
			break;
		case Irecv:
			//printf("message type is Irecv\n");
			//printf("message type is Irecv,typesize is %d, recvcount is %d, source is %d, tag is %d, comm is %d, datatype is %d\n",rmr->typesize,rmr->recvcount,rmr->source, rmr->tag, rmr->comm, rmr->datatype);
			fread(buf,rmr->typesize,rmr->recvcount,file);
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
	return 1;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
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

void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    ////printf("in poll_cq\n");
    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct server_context *s_context;

  ////printf("received connection request.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = s_context = (struct server_context *)malloc(sizeof(struct server_context));
  //conn->qp = id->qp;

  register_memory(s_context,BUFFER_SIZE,BUFFER_SIZE);
  post_receives(id);
  build_params(&cm_params);
  //memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

int on_connection(struct rdma_cm_id *id)
{
  //struct server_context *s_context = (struct server_context *)id->context;

  //RdmaMRes *rdmamres = (RdmaMRes *)malloc(ressize);
  //rdmamres->MsgType = Start;
  //memcpy(s_context->send_region,rdmamres,ressize);
  //write_remote(id,BUFFER_SIZE);

  //free(rdmamres);
  //printf("leave on_connection\n");
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct server_context *s_context = (struct server_context *)id->context;

  //printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  //ibv_dereg_mr(s_context->send_mr);
  //ibv_dereg_mr(s_context->recv_mr);

  //free(s_context->send_region);
  //free(s_context->msg);
  deregister_memory(s_context);

  free(s_context);

  rdma_destroy_id(id);

  return 1;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
  {
    r = on_connect_request(event->id);
    ////printf("connect_request\n");
  }
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
  {
    r = on_connection(event->id);
    ////printf("event_established\n");
  }
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
  {
    //printf("event disconnected\n");
    r = on_disconnect(event->id);
  }
  else
    die("on_event: unknown event.");

  return r;
}

/*int post_send(struct rdma_cm_id *id,void *res, void *buf, int headlength,int contentlength)
{
  ////printf("enter post_send,%d\n",++send_time);
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;


  memcpy(conn->send_region,res,headlength);
  if(contentlength)
  {
	  memcpy(conn->send_region + headlength, buf, contentlength);
  }
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  sge.length = BUFFER_SIZE;
  //sge.length = headlength + contentlength;
  sge.lkey = conn->send_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
  
  return 0;
}*/

void post_receives(struct rdma_cm_id *id)
{
  ////printf("enter post_receives,%d\n", ++recv_time);
  struct server_context *s_context = (struct server_context *)id->context;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = (uintptr_t)id;
  //wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)s_context->msg;
  sge.length = MESSAGE_SIZE;
  sge.lkey = s_context->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  ////printf("leave post_receives\n");
}

static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
  struct server_context *s_context = (struct server_context *)id->context;

  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.imm_data = 1000;
  wr.wr.rdma.remote_addr = s_context->peer_addr;
  wr.wr.rdma.rkey = s_context->peer_rkey;

  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)s_context->send_region;
  sge.length = len;
  sge.lkey = s_context->send_mr->lkey;

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
}

void register_memory(struct server_context *s_context, int lengthSend, int lengthRecv)
{
  //conn->send_region = malloc(lengthSend);
  //conn->recv_region = malloc(lengthRecv);
  posix_memalign((void **)&s_context->send_region, sysconf(_SC_PAGESIZE), BUFFER_SIZE);

  posix_memalign((void **)&s_context->msg, sysconf(_SC_PAGESIZE), MESSAGE_SIZE);

  TEST_Z(s_context->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    s_context->send_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
  

  TEST_Z(s_context->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    s_context->msg, 
    MESSAGE_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}


void deregister_memory(struct server_context *s_context)
{
  
  ibv_dereg_mr(s_context->send_mr);
  ibv_dereg_mr(s_context->recv_mr);

  free(s_context->send_region);
  free(s_context->msg);
  
}

void on_completion(struct ibv_wc *wc)
{
  ////printf("status is %d,opcode is %d\n", wc->status,wc->opcode);
  struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
  struct server_context *s_context = (struct server_context *)id->context;
  if (wc->status != IBV_WC_SUCCESS)
  {
    printf("status is %d,opcode is %d\n", wc->status,wc->opcode);	
    //deregister_memory(s_context);
    die("on_completion: status is not IBV_WC_SUCCESS.");
  }
  if (wc->opcode ==  IBV_WC_RECV) {
    ////printf("enter IBV_WC_RECV,%d\n",recv_time);

    ////printf("received message: %s\n", conn->recv_region);
    RdmaMReq *rdmamreq = (RdmaMReq *)malloc(reqsize);
    memcpy(rdmamreq,&(s_context->msg->rdmamreq),reqsize);
    MSGL *msgl = (MSGL *)malloc(sizeof(MSGL));
    RdmaMRes *rdmamres = (RdmaMRes *)malloc(ressize);
    MSGCWOS *msgcwos;
    MSGCWS *msgcws;
    int sendlength = 0;
    int head = 0;
    ////printf("request type is %d\n",rdmamreq->MsgType);
    /*if(communication_num > 0)
    {
	    post_receives(conn,BUFFER_SIZE);
	    communication_num--;
    }*/
	if(rdmamreq->MsgType == ReadMsg)
	{
		PassOneMessage();
		return;
	}
    switch(rdmamreq->MsgType)
    {
	case None:
		////printf("None\n");
  		post_receives(id);
		break;
	case Start:
    		//printf("enter start\n");
    		/*if(communication_num > 0)
    		{
    			//printf("expected recv_num is %d,communication_current is %d\n", expectedRecvNum[communication_current],communication_current);
	    		for(int i=0;i<expectedRecvNum[communication_current];i++)
				post_receives(conn,BUFFER_SIZE);
	    		communication_num--;
			communication_current++;
    		}*/
		s_context->peer_addr = s_context->msg->mr.addr;
		s_context->peer_rkey = s_context->msg->mr.rkey;
                
		rdmamres->MsgType = Start;
  		memcpy(s_context->send_region,rdmamres,ressize);
		post_receives(id);		
		write_remote(id,ressize);
    		////printf("write_remote\n");
    		////printf("leave start\n");
		break;
        case Stop:
                //printf("enter Stop\n");
                rdmamres->MsgType = Stop;
                memcpy(s_context->send_region,rdmamres,ressize);
                write_remote(id,ressize);
		fclose(file);
                //post_receives(id);
                break;
	case Send:
    		//printf("enter send\n");
		if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		msgcwos = (MSGCWOS *)myHashMapGetDataByKey(SendTable,msgl);
		rdmamres->MsgType = Send;
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->returnvalue = msgcwos->returnvalue;
    		////printf("Send:returnvalue is %d\n", rdmamres->returnvalue);
		rdmamres->time = msgcwos->time;
		rdmamres->totalsize = 0;
    		/*if(communication_num > 0)
    		{
    			//printf("Send:expected recv_num is %d,communication_current is %d,communication_num is %d\n", expectedRecvNum[communication_current],communication_current,communication_num);
	    		for(int i=0;i<expectedRecvNum[communication_current];i++)
				post_receives(conn,BUFFER_SIZE);
	    		communication_num--;
			communication_current++;
    		}*/
    		////printf("before copy\n");
  		memcpy(s_context->send_region,rdmamres,ressize);
  		post_receives(id);
		write_remote(id,ressize);
		//write_remote(id,BUFFER_SIZE);
				
		myHashMapRemoveDataByKey(SendTable,msgl);
		/*if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}*/
    		//printf("leave send\n");
		break;
	case Isend:
    		//printf("enter Isend\n");
		if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		msgcws = (MSGCWS *)myHashMapGetDataByKey(IsendTable,msgl);
		rdmamres->MsgType = Isend;
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->returnvalue = msgcws->returnvalue;
		rdmamres->status = msgcws->status;
    		////printf("Send:returnvalue is %d\n", rdmamres->returnvalue);
		rdmamres->time = msgcws->time;
		rdmamres->totalsize = 0;
    		/*if(communication_num > 0)
    		{
    			//printf("Send:expected recv_num is %d,communication_current is %d,communication_num is %d\n", expectedRecvNum[communication_current],communication_current,communication_num);
	    		for(int i=0;i<expectedRecvNum[communication_current];i++)
				post_receives(conn,BUFFER_SIZE);
	    		communication_num--;
			communication_current++;
    		}*/
    		////printf("before copy\n");
  		memcpy(s_context->send_region,rdmamres,ressize);
  		post_receives(id);
		write_remote(id,ressize);
		//write_remote(id,BUFFER_SIZE);
				
		myHashMapRemoveDataByKey(IsendTable,msgl);
		/*if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}*/
    		//printf("leave Isend\n");
		break;
	case Issend:
    		//printf("enter Isend\n");
		if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		msgcws = (MSGCWS *)myHashMapGetDataByKey(IssendTable,msgl);
		rdmamres->MsgType = Issend;
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->returnvalue = msgcws->returnvalue;
		rdmamres->status = msgcws->status;
    		////printf("Send:returnvalue is %d\n", rdmamres->returnvalue);
		rdmamres->time = msgcws->time;
		rdmamres->totalsize = 0;
    		/*if(communication_num > 0)
    		{
    			//printf("Send:expected recv_num is %d,communication_current is %d,communication_num is %d\n", expectedRecvNum[communication_current],communication_current,communication_num);
	    		for(int i=0;i<expectedRecvNum[communication_current];i++)
				post_receives(conn,BUFFER_SIZE);
	    		communication_num--;
			communication_current++;
    		}*/
    		////printf("before copy\n");
  		memcpy(s_context->send_region,rdmamres,ressize);
  		post_receives(id);
		write_remote(id,ressize);
		//write_remote(id,BUFFER_SIZE);
				
		myHashMapRemoveDataByKey(IssendTable,msgl);
		/*if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}*/
    		//printf("leave Isend\n");
		break;
	case Recv:
    		//printf("enter Recv\n");
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcws = (MSGCWS *)myHashMapGetDataByKey(RecvTable,msgl);
		//if(msgcws == NULL) printf("Recv: msgcws is null\n");
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->MsgType = Recv;
		rdmamres->returnvalue = msgcws->returnvalue;
		rdmamres->time = msgcws->time;
		rdmamres->status = msgcws->status;
		sendlength = msgcws->totalsize;
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcws->buf + head,rdmamres->totalsize);
  		post_receives(id);
		write_remote(id,ressize + rdmamres->totalsize);
		//write_remote(id,BUFFER_SIZE);
		
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
		{
			free(msgcws->buf);
			myHashMapRemoveDataByKey(RecvTable,msgl);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
    				//printf("leave recv\n");
			}*/	
		}

    		////printf("leave Recv\n");
		break;
	case Irecv:
    		//printf("enter Irecv\n");
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
    		//printf("Irecv: source is %d, tag is %d, comm is %d, datatype is %d\n", msgl->source,msgl->tag,msgl->comm,msgl->datatype);
		msgcws = (MSGCWS *)myHashMapGetDataByKey(IrecvTable,msgl);
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->MsgType = Irecv;
		rdmamres->returnvalue = msgcws->returnvalue;
		rdmamres->time = msgcws->time;
		rdmamres->totalsize = msgcws->totalsize;
		rdmamres->status = msgcws->status;
		sendlength = msgcws->totalsize;
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
        if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("irecv totalsize is %d, head is %d, blocksize is %d\n",sendlength,rdmamreq->head, BLOCK_SIZE);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcws->buf + head,rdmamres->totalsize);
		
    		////printf("before write\n");
  		post_receives(id);
		write_remote(id,ressize + rdmamres->totalsize);
		//write_remote(id,BUFFER_SIZE);
		
        if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		{
			free(msgcws->buf);
			myHashMapRemoveDataByKey(IrecvTable,msgl);
			
			/*if(ifReadFile == 1)
			{
				
    				//printf("before readmessage\n");
				ifReadFile = ReadOneMessage();
			}*/
		}
    		//printf("leave Irecv\n");
		break;
	case Iprobe:
    		//printf("enter Isend\n");
		if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}
		msgl->source = rdmamreq->source;
		msgl->tag = rdmamreq->tag;
		msgl->comm = rdmamreq->comm;
		msgl->datatype = rdmamreq->datatype;
		msgcws = (MSGCWS *)myHashMapGetDataByKey(IprobeTable,msgl);
		rdmamres->MsgType = Iprobe;
		rdmamres->source = rdmamreq->source;
		rdmamres->tag = rdmamreq->tag;
		rdmamres->comm = rdmamreq->comm;
		rdmamres->datatype = rdmamreq->datatype;
		rdmamres->returnvalue = msgcws->returnvalue;
		rdmamres->status = msgcws->status;
    		////printf("Send:returnvalue is %d\n", rdmamres->returnvalue);
		rdmamres->time = msgcws->time;
		rdmamres->totalsize = msgcws->totalsize;
    		/*if(communication_num > 0)
    		{
    			//printf("Send:expected recv_num is %d,communication_current is %d,communication_num is %d\n", expectedRecvNum[communication_current],communication_current,communication_num);
	    		for(int i=0;i<expectedRecvNum[communication_current];i++)
				post_receives(conn,BUFFER_SIZE);
	    		communication_num--;
			communication_current++;
    		}*/
    		////printf("before copy\n");
  		memcpy(s_context->send_region,rdmamres,ressize);
  		post_receives(id);
		write_remote(id,ressize);
		//write_remote(id,BUFFER_SIZE);
				
		myHashMapRemoveDataByKey(IprobeTable,msgl);
		/*if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}*/
    		//printf("leave Isend\n");
		break;
	case Reduce:
    		//printf("enter Reduce\n");
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(ReduceList);
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->totalsize = msgcwos->totalsize;
		rdmamres->time = msgcwos->time;
		rdmamres->MsgType = Reduce;
		sendlength = msgcwos->totalsize;
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
  		post_receives(id);
		
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(ReduceList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Reduce_NR:
    		////printf("enter Reduce_NR\n");
					
		if(ifReadFile == 1)
		{	
			ifReadFile = ReadOneMessage();
		}
		msgcwos = myListGetDataAtFirst(ReduceList);
		rdmamres->MsgType = Reduce_NR;
		rdmamres->totalsize = 0;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
  		memcpy(s_context->send_region,rdmamres,ressize);
		write_remote(id,ressize);
  		post_receives(id);
		myListRemoveDataAtFirst(ReduceList);
		/*if(ifReadFile == 1)
		{
			ifReadFile = ReadOneMessage();
		}*/
		break;
	case Allreduce:
    		////printf("enter Allreduce\n");
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(AllreduceList);
		rdmamres->MsgType = Allreduce;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
		//rdmamres->totalsize = msgcwos->totalsize;
		sendlength = msgcwos->totalsize;
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
  		post_receives(id);
		
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(AllreduceList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Alltoall:
    		//printf("enter alltoall,head is %d\n", rdmamreq->head);
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(AlltoallList);
		rdmamres->MsgType = Alltoall;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
		sendlength = msgcwos->totalsize;
    		//printf("Alltoall totalsize is %d,returnvalue is %d\n",sendlength,msgcwos->returnvalue);
    		////printf("Alltoallv totalsize is %d,head is %d\n",sendlength,rdmamreq->head);
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
    		printf("Alltoall time is %lf\n",msgcwos->time);
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
    		//printf("alltoall,totalsize is %d\n", rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
		
    		//printf("alltoall,write remote\n");
  		post_receives(id);
		
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(AlltoallList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Alltoallv:
    		//printf("enter alltoallv\n");
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(AlltoallvList);
		rdmamres->MsgType = Alltoallv;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
		//rdmamres->totalsize = msgcwos->totalsize;
		sendlength = msgcwos->totalsize;
    		printf("Alltoallv time is %lf\n",msgcwos->time);
    		////printf("Alltoallv totalsize is %d,head is %d\n",sendlength,rdmamreq->head);
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
  		post_receives(id);
		
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(AlltoallvList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Bcast:
    		//printf("enter Bcast\n");
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(BcastList);
		rdmamres->MsgType = Bcast;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
		//rdmamres->totalsize = msgcwos->totalsize;
		sendlength = msgcwos->totalsize;
    		////printf("Alltoallv totalsize is %d,returnvalue is %d\n",sendlength,msgcwos->returnvalue);
    		////printf("Alltoallv totalsize is %d,head is %d\n",sendlength,rdmamreq->head);
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
    		////printf("Bcast,sendlength is %d\n", sendlength);
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
  		post_receives(id);
		
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(BcastList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Gather:
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(GatherList);
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->totalsize = msgcwos->totalsize;
		rdmamres->time = msgcwos->time;
		rdmamres->MsgType = Gather;
		sendlength = msgcwos->totalsize;
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) < BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
  		post_receives(id);
		
        if((sendlength - rdmamreq->head) < BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(GatherList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	case Gather_NR:
		if(ifReadFile == 1)
		{	
			ifReadFile = ReadOneMessage();
		}
		msgcwos = myListGetDataAtFirst(GatherList);
		rdmamres->MsgType = Gather_NR;
		rdmamres->totalsize = 0;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
  		memcpy(s_context->send_region,rdmamres,ressize);
		write_remote(id,ressize);
  		post_receives(id);
		myListRemoveDataAtFirst(GatherList);
		break;
	case Allgather:
		if(rdmamreq->head==0)
		{			
			if(ifReadFile == 1)
			{	
				ifReadFile = ReadOneMessage();
			}
		}
		msgcwos = myListGetDataAtFirst(AllgatherList);
		rdmamres->MsgType = Allgather;
		rdmamres->returnvalue = msgcwos->returnvalue;
		rdmamres->time = msgcwos->time;
		sendlength = msgcwos->totalsize;
    		//printf("Alltoall totalsize is %d,returnvalue is %d\n",sendlength,msgcwos->returnvalue);
    		////printf("Alltoallv totalsize is %d,head is %d\n",sendlength,rdmamreq->head);
		rdmamres->head = rdmamreq->head;
		rdmamres->totalsize = BLOCK_SIZE;
                
                if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
			rdmamres->totalsize = sendlength - rdmamreq->head;
		 
    		////printf("Alltoallv totalsize is %d,send %d this time\n",sendlength,rdmamres->totalsize);

  		memcpy(s_context->send_region,rdmamres,ressize);
  		memcpy(s_context->send_region + ressize, msgcwos->buf + head,rdmamres->totalsize);
    		//printf("alltoall,totalsize is %d\n", rdmamres->totalsize);
		write_remote(id,ressize + rdmamres->totalsize);
		
    		//printf("alltoall,write remote\n");
  		post_receives(id);
		
        if((sendlength - rdmamreq->head) <= BLOCK_SIZE)
		{
			free(msgcwos->buf);
			myListRemoveDataAtFirst(AllgatherList);
			/*if(ifReadFile == 1)
			{
				ifReadFile = ReadOneMessage();
			}*/
		}
		break;
	default:
		break;
    }
    free(msgl);
    free(rdmamres);
    free(rdmamreq);
    /*if(communication_num > 0)
    {
	    //usleep(500000);
	    post_receives(conn,BUFFER_SIZE);
	    communication_num--;
    }*/
  } else if (wc->opcode == IBV_WC_SEND) {
    	////printf("send completed successfully,%d.\n",send_time);
  }
}

