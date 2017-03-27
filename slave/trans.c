/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include <malloc.h>
#include <sys/time.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/socket.h>
#include "trans.h"
#include "thread_global.h"
#include "data_record.h"
#include "lock_record.h"
#include "mem.h"
#include "proc.h"
#include "data_am.h"
#include "transactions.h"
#include "config.h"
#include "thread_main.h"
#include "socket.h"
#include "communicate.h"
#include "server_data.h"
#include "state.h"
#include "transactions.h"

static IDMGR* CentIdMgr;

//the max number of transaction ID for per process.
int MaxTransId=100000;

TransactionId thread_0_tid;

void InitTransactionIdAssign(void)
{
    Size size;
    size=sizeof(IDMGR);

    CentIdMgr=(IDMGR*)malloc(size);

    if(CentIdMgr==NULL)
    {
        printf("malloc error for IdMgr.\n");
        return;
    }

    CentIdMgr->curid=nodeid*THREADNUM*MaxTransId + 1;

    pthread_mutex_init(&(CentIdMgr->IdLock), NULL);
}

void ProcTransactionIdAssign(THREAD* thread)
{
    int index;
    index=thread->index;

    thread->maxid=(index+1)*MaxTransId;
}

TransactionId AssignTransactionId(void)
{
    TransactionId tid;

    THREAD* threadinfo;

    threadinfo=pthread_getspecific(ThreadInfoKey);

    if(threadinfo->curid<=threadinfo->maxid)
        tid=threadinfo->curid++;
    else
        return 0;
    return tid;
}

//get the transaction id, add proc array and get the snapshot from the master node.
void StartTransactionGetData(void)
{
    TransactionData* td;

    td=(TransactionData*)pthread_getspecific(TransactionDataKey);

    td->tid=AssignTransactionId();
    if(!TransactionIdIsValid(td->tid))
    {
        printf("transaction ID assign error.\n");
        return;
    }
}

void InitTransactionStructMemAlloc(void)
{
    TransactionData* td;
    THREAD* threadinfo;
    char* memstart;
    Size size;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    memstart=threadinfo->memstart;

    size=sizeof(TransactionData);

    td=(TransactionData*)MemAlloc((void*)memstart,size);

    if(td==NULL)
    {
        printf("memalloc error.\n");
        return;
    }

    pthread_setspecific(TransactionDataKey,td);

    // to set data memory.
    InitDataMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
    // to set data memory.
    InitDataMem();

    StartTransactionGetData();
}

void CommitTransaction(void)
{
    CommitDataRecord();

    //TransactionMemClean();
}

void AbortTransaction(void)
{
    // process array clean should be after function 'AbortDataRecord'.

    AbortDataRecord();

    //TransactionMemClean();
}

void ReleaseConnect(void)
{
    int i;
    THREAD* threadinfo;
    int index2;
    int conn;
    uint64_t* sbuffer;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    index2=threadinfo->index;
    int lindex;
    lindex = GetLocalIndex(index2);

    sbuffer=send_buffer[lindex];

    for (i = 0; i < nodenum; i++)
    {
        conn=connect_socket[i][lindex];

        //send data-insert to node "i".
        *(sbuffer) = cmd_release;

        int num = 1;
        Send(conn, sbuffer, num);
    }
}

void DataReleaseConnect(void)
{
    int conn;
    uint64_t* sbuffer;

    sbuffer=send_buffer[0];

    conn=connect_socket[nodeid][0];

    // send data-insert to node itself.
    *(sbuffer) = cmd_release;

    int num = 1;
    Send(conn, sbuffer, num);
}

/*
void TransactionLoadData(int i)
{
    int j;
    int result;
    int index;
    for (j = 0; j < 20; j++)
    {
      StartTransaction();
      Data_Insert(2, j+1, j+1, nodeid);
      result=PreCommit(&index);
      if(result == 1)
      {
          CommitTransaction();
      }
      else
      {
            //current transaction has to rollback.
            AbortTransaction(index);
      }
    }
    DataReleaseConnect();
    ResetProc();
    ResetMem(0);
}
*/
void TransactionRunSchedule(void* args)
{
    //to run transactions according to args.
    int type;
    int rv;
    terminalArgs* param=(terminalArgs*)args;
    type=param->type;

    THREAD* threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

    if(type==0)
    {
        printf("begin LoadData......\n");
        //LoadData();
        //smallbank
        //LoadBankData();
        switch(benchmarkType)
        {
        case TPCC:
      	  LoadData();
      	  break;
        case SMALLBANK:
      	  LoadBankData();
      	  break;
        default:
      	  printf("benchmark not specified\n");
        }

        DataReleaseConnect();
        thread_0_tid=threadinfo->curid;
        ResetMem(0);
        ResetProc();
    }
    else
    {
        printf("ready to execute transactions...\n");

        rv=pthread_barrier_wait(&barrier);
        if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
        {
            printf("Couldn't wait on barrier\n");
            exit(-1);
        }


        printf("begin execute transactions...\n");
        //executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
        //smallbank
        //executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
        switch(benchmarkType)
        {
        case TPCC:
      	  executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
      	  break;
        case SMALLBANK:
      	  executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
      	  break;
        default:
      	  printf("benchmark not specified\n");
        }
        ReleaseConnect();
    }
}

void LocalCommitTransaction(int index, TimeStampTz ctime)
{
    LocalCommitDataRecord(index, ctime);

    DataLockRelease(index);
    ResetServerdata(index);
    // transaction have over.
    setTransactionState(index, inactive);
}

void LocalAbortTransaction(int index, int trulynum)
{
    ServerData* data;
    data = serverdata + index;
    // in some status, transaction is failed before the truly manipulate the data record.
    if (data->lock_num == 0)
    {
        setTransactionState(index, aborted);
        ResetServerdata(index);
    }
    else
    {
        // need not broadcast the read transactions because now must be a transaction that not arrive the prepared state.
        LocalAbortDataRecord(index, trulynum);
        // wake up the read transaction
        setTransactionState(index, aborted);
        DataLockRelease(index);
        ResetServerdata(index);
    }
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int LocalPreCommit(int* number, int index, TransactionId tid)
{
    LocalDataRecord*start;
    int num,i,result = 1;
    LocalDataRecord* ptr;

    start=(serverdata+index)->datarecord;

    num=(serverdata+index)->data_num;

    // sort the data-operation records.
    LocalDataRecordSort(start, num);

    for(i=0;i<num;i++)
    {
        ptr=start+i;

        switch(ptr->type)
        {
            case DataInsert:
                result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, index, tid);
                break;
            case DataUpdate:
                result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, index, tid);
                break;
            case DataDelete:
                result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, index, tid);
                break;
            default:
                printf("PreCommit:shouldn't arrive here.\n");
        }
        if(result == -1)
        {
            //return to roll back.
            *number=i;
            return -1;
        }
    }
    setTransactionState(index, prepared);
    return 1;
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(void)
{
    TransactionId tid;
    int index;
    char* DataMemStart, *start;
    TransactionData* tdata;
    int num,i;
    DataRecord* ptr;
    THREAD* threadinfo;
    int lindex;

    uint64_t* sbuffer;
    uint64_t* rbuffer;
    int conn;

    DataMemStart=(char*)pthread_getspecific(DataMemKey);
    start=DataMemStart+DataNumSize;
    num=*(int*)DataMemStart;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    index=threadinfo->index;
    lindex = GetLocalIndex(index);

    sbuffer=send_buffer[lindex];
    rbuffer=recv_buffer[lindex];

    tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
    tid=tdata->tid;

    // sort the data-operation records.
    DataRecordSort((DataRecord*)start, num);

    bool t_is_abort = false;
    bool is_abort = false;

    for(i=0;i<num;i++)
    {
        ptr=(DataRecord*)(start+i*sizeof(DataRecord));

        conn=connect_socket[ptr->node_id][lindex];

        //send data-insert to node "nid".
        *(sbuffer) = cmd_prepare;
        *(sbuffer+1) = tid;

        int num = 2;
        Send(conn, sbuffer, num);

        // response from "nid".
        num = 1;
        Receive(conn, rbuffer, num);

        is_abort = *(recv_buffer[lindex]);

        if (is_abort)
        {
            t_is_abort = true;
        }
    }

    if (t_is_abort)
    {
        return -1;
    }
    else
    {
        return 1;
    }
}

int GetNodeId(int index)
{
   return (index/threadnum);
}

int GetLocalIndex(int index)
{
    return (index%threadnum);
}
