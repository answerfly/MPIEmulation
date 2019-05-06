#include "shm_common.h"
#include <errno.h>
#include <malloc.h>
#include <unistd.h>
int main()
{
	int shmid = create_shm(getpagesize()*sizeof(char),50);
	printf("shmid is %d\n", shmid);
	//void *shmbuf = malloc(4096*sizeof(char));
	//void *shmbuf = memalign(getpagesize(),1);
	void *shmbuf;
	//posix_memalign((void **)&shmbuf, 256,1024 );
	int i = 0;
	//shmat(shmid,shmbuf, 0);
	shmbuf = shmat(shmid, NULL, 0);
	perror("shmat");
	shmdt(shmbuf);
	shmat(shmid, shmbuf, 0);
	perror("shmat");

	char *buf = (char *)shmbuf;
	while( i < 4096)
	{
		buf[i] = 'a'+i ;
		buf[i+1] = '\0';
		i++;
		sleep(1); 
		if(i == 26)
		{
			buf[i] = '\0';
			break; // 让程序结束，去释放该共享内存
		}
	}
	destroy_shm(shmid);
	return 0;
}
