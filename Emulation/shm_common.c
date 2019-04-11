#include "shm_common.h"
static int shm_common(int size, int shmflag, int id)
{
	    key_t key = ftok(PATHNAME, id); // 获取key 
		if(key < 0)
		{
				perror("lf:ftok");
				return -1;
		}
		int shmid = shmget(key,  size, shmflag);
		if(shmid < 0)
		{
				perror("shmget");
				return -2;
		}
		return shmid;
}

int create_shm( int size, int id)
{
	    return shm_common(size, IPC_CREAT|IPC_EXCL|0666, id);
}

int get_shmid(int id)
{
	    return shm_common(0, IPC_CREAT, id);
}

int destroy_shm(int shmid)
{
	    if( shmctl( shmid, IPC_RMID, NULL) < 0)
		{
				perror("shmctl");
				return -1;
		}
		return 0;
}
