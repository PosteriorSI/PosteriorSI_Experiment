#include<stdlib.h>
#include "state.h"
#include "socket.h"
#include "trans.h"
#include "config.h"

TransactionStateData* DatabaseState;

void InitDatabaseState(void)
{
    int size = (NODENUM*THREADNUM+1)*sizeof(TransactionStateData);

    DatabaseState = (TransactionStateData*)malloc(size);
    if (DatabaseState == NULL)
    {
        printf("atebase state malloc error.\n");
        return;
    }
    TransactionStateData* p;
    int i;
    for (i = 0, p = DatabaseState; i < NODENUM*THREADNUM+1; i++, p++)
    {
        p->state = inactive;
    }
}

void InitTransactionState(int index, TransactionId tid, TimeStampTz snapshot_time)
{
    TransactionStateData* state = DatabaseState + index;

    state->state = active;
}

TransactionState getTransactionState(int index)
{
    TransactionState result;
    TransactionStateData* state;
    state = DatabaseState + index;
    result = state->state;
    return result;
}

void setTransactionState(int index, TransactionState wstate)
{
    TransactionStateData* state;
    state = DatabaseState + index;
    state->state = wstate;
}

void freeState(void)
{
    free(DatabaseState);
}
