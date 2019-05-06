gcc -c myRecordWithTime.c myHashCode.c myHashMap.c myList.c myEqual.c myMessageWithTime.c -std=c99 -I/home/mpi/include
ar cr librecord.a myRecordWithTime.o myHashCode.o myList.o myHashMap.o myEqual.o myMessageWithTime.o
gcc -g -c myEmulationMPI.c myHashCode.c myHashMap.c myList.c myEqual.c myMessageWithTime.c shm_common.c -std=gnu99 -I/home/mpi/include -L/home/mpi/lib -lrdmacm -lpthread -libverbs
ar cr libmpiemulation.a myEmulationMPI.o myHashCode.o myList.o myHashMap.o myEqual.o myList.o myMessageWithTime.o shm_common.o
cp librecord.a ../lib
cp libmpiemulation.a ../lib
