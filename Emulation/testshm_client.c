#include "shm_common.h"

int main()
{
	int shmid = get_shmid(PROJ_ID);
	printf("client:shmid is %d\n", shmid);
	//char *buf = (char *)malloc(27 * sizeof(char));
	int index = 0;
	char *buf = shmat(shmid, NULL, 0);

	while(index < 4096)
	{
		printf("%s\n", buf);
		sleep(1);
		index++;
		if(index == 27)
			break; 
	}
	return 0;
}
