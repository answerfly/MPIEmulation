#include<sys/time.h>
#include<unistd.h>
#include<netdb.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include"mpi.h"
#include"myMessageWithTime.h"



int get_comm_num(MPI_Comm comm, int *num);
int ReadOneMessage();



int IfFileOpen = 0;
//static int IfSelected = 0;
//FILE *file;
FILE *listfile;
FILE *addrfile;
FILE *file;
int ProcessID;
int PortID;
int comm_size;
//int send_time = 0;
//int recv_time = 0;
int sleepus = 50;
int request_static = 1;


int MPI_Comm_size(MPI_Comm comm, int *size)
{
	*size = comm_size;
	int size_tmp;
	PMPI_Comm_size(MPI_COMM_WORLD,&size_tmp);
	printf("real_mpi_comm_size, commsize is %d\n", size_tmp);
	return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{	
	*rank = ProcessID;
	int rank_tmp;
	PMPI_Comm_rank(MPI_COMM_WORLD,&rank_tmp);
	printf("real_mpi_comm_rank, rank is %d\n", rank_tmp);
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
	//char *argv_tmp[] = *argv;
	printf("enter init\n");
	if((listfile=fopen("ProcessList","r"))==NULL) 
	{
		return 0;
	}
	/*if((addrfile=fopen("Addr_Port_File","r"))==NULL) 
	{
		return 1;
	}*/
	
	char tempList[20];
	if(fgets(tempList,128,listfile)!=NULL)
	{
		comm_size = char2int(tempList);
	}
	printf("init:commsize is %d\n", comm_size);

	printf("init:argv is %s\n", (*argv)[1]);
	//int id = 0;
	int id = char2int((*argv)[1]);
	int flag = 0;
	int strlength;
	while((fgets(tempList,128,listfile))!=NULL)
	{
		if(id==flag)
		{
			ProcessID  = char2int(tempList);
			printf("init : id is %d\n", ProcessID);
			fgets(tempList,128,listfile);
			//const char *address_tmp = address;
			//printf("address is %s\n", address);
			fgets(tempList,128,listfile);
			//const char *port_tmp = port;
			//printf("port is %s\n", port);
			break;
		}
		
		fgets(tempList,128,listfile);
		fgets(tempList,128,listfile);
		flag++;	
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
	PMPI_Init(argc,argv);
	printf("%d : leave init\n",ProcessID);
	return 0;
}

int MPI_Send(void *buf,int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
	printf("%d : enter Send\n",ProcessID);
	//printf("%d : enter Send,dest is %d, tag is %d, type is %d, comm is %d\n",ProcessID, *dest,*tag,*datatype,*comm);
	printf("%d : leave Send\n",ProcessID);
	return 0;
}


int MPI_Recv(void *buf,int count,MPI_Datatype datatype,int source,int tag,MPI_Comm comm,MPI_Status *status)
{
	printf("%d : enter Recv\n",ProcessID);

	////printf("%d : enter Recv:source is %d, tag is %d, comm is %d ,datatype is %d,count is %d\n",ProcessID,*source,*tag,*comm,*datatype,*count);
	int type_size;
    MPI_Type_size(datatype, &type_size);
	printf("%d : Recv: type size is %d\n",ProcessID,type_size);
	memset(buf,65, type_size*count);
	return 0;
}


int MPI_Finalize()
{
	printf("%d: enter finalize\n",ProcessID);
	
	PMPI_Finalize();
	
	printf("%d: leave finalize\n",ProcessID);
	return 0;
}
