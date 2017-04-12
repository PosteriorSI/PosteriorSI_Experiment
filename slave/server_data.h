#ifndef SERVER_RECORD_
#define SERVER_RECORD_

#include "local_data_record.h"
#include "lock_record.h"

typedef struct ServerData {
    int data_num;
    int lock_num;
    LocalDataRecord* datarecord;
    DataLock* lockrecord;
} ServerData;

extern ServerData* serverdata;

extern void MallocServerData(void);

extern void InitServerData(void);

extern void ResetServerdata(int index);

extern void freeServerData(void);
#endif
