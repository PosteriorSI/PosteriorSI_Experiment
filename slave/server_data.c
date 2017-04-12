#include <stdio.h>
#include "socket.h"
#include "config.h"
#include "server_data.h"
#include "proc.h"

ServerData* serverdata;

void MallocServerData(void)
{
   serverdata = (ServerData*)malloc((NODENUM*THREADNUM+1)*sizeof(ServerData));
   if (serverdata == NULL)
   {
       printf("server data malloc error\n");
   }
   ServerData* p;
   int i;
   for (i = 0, p = serverdata; i < NODENUM*THREADNUM+1; i++, p++)
   {
       p->datarecord = (LocalDataRecord*)malloc(MaxDataRecordNum*sizeof(LocalDataRecord));
       p->lockrecord = (DataLock*)malloc(MaxDataLockNum*sizeof(DataLock));
       if (p->datarecord == NULL || p->lockrecord == NULL)
       {
           printf("local data record or local data lock malloc error\n");
       }
   }
   // must initialize for the first time
   InitServerData();
}

void InitServerData(void)
{
    ServerData* data;
    int i;
    int size_data;
    int size_lock;
    size_data = MaxDataRecordNum*sizeof(LocalDataRecord);
    size_lock = MaxDataLockNum*sizeof(DataLock);
    for (i = 0, data = serverdata; i < NODENUM*THREADNUM+1; i++, data++)
    {
        data = serverdata + i;
        data->data_num = 0;
        data->lock_num = 0;
        memset(data->datarecord, 0, size_data);
        memset(data->lockrecord, 0, size_lock);
    }
}


void ResetServerdata(int index)
{
    ServerData* data;
    data = serverdata + index;
    int size_lock;
    data->data_num = 0;
    data->lock_num = 0;
    size_lock = MaxDataLockNum*sizeof(DataLock);
    //memset(data->datarecord, 0, size_data);
    memset(data->lockrecord, 0, size_lock);
}

void freeServerData(void)
{
    ServerData* p;
    int i;
    for (i = 0, p = serverdata; i < NODENUM*THREADNUM+1; i++, p++)
    {
        free(p->datarecord);
        free(p->lockrecord);
    }

    free(serverdata);
}
