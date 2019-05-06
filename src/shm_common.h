#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/ipc.h>
#include<sys/shm.h>

#define PATHNAME "/tmp" // ftok函数 生成key使用
#define PROJ_ID 67 // ftok 函数生成key使用

int create_shm(int size, int id);
int destroy_shm(int shmid);
int get_shmid(int id);

