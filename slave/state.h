/* This file is some structure and manipulation about the state of the
 * current transaction
 * */
#ifndef STATE_H_
#define STATE_H_

#include <pthread.h>
#include "timestamp.h"
#include "local_data_record.h"
#include "lock_record.h"

typedef enum TransactionState {
    inactive,
    active,
    committing,
    prepared,
    committed,
    aborted
} TransactionState;

/* wait for more member */
typedef struct TransactionStateData {
    TransactionState state;
} TransactionStateData;

// record all the states of the current transactions, and store the states
// in the shared memory
extern TransactionStateData* DatabaseState;
extern void InitDatabaseState(void);
extern TransactionState getTransactionState(int index);
extern void setTransactionState(int index, TransactionState wstate);

extern void freeState(void);

#endif
