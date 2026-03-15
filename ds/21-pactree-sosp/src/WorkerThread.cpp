// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "WorkerThread.h"

#include <assert.h>
#include <libpmemobj.h>

#include "Combiner.h"
#include "Oplog.h"
#include "pactree.h"

WorkerThread::WorkerThread(int id, int activeNuma) {
    this->workerThreadId = id;
    this->activeNuma = activeNuma;
    this->workQueue = &g_workQueue[workerThreadId];
    this->logDoneCount = 0;
    this->opcount = 0;
    if (id == 0) freeQueue = new std::queue<std::pair<uint64_t, void *>>;
}

bool WorkerThread::applyOperation() {
    std::vector<OpStruct *> *oplog = workQueue->front();
    int numaNode = workerThreadId % activeNuma;
    SearchLayer *sl = g_perNumaSlPtr[numaNode];
    uint8_t hash = static_cast<uint8_t>(workerThreadId / activeNuma);
    bool ret = false;
    for (auto opsPtr : *oplog) {
        OpStruct &ops = *opsPtr;
        if (ops.hash != hash) continue;
        opcount++;
        if (ops.op == OpStruct::insert) {
            #if defined(USE_PMDK)
            void *newNodePtr = reinterpret_cast<void *>(
                ((unsigned long)(ops.poolId)) << 48 | (ops.newNodeOid.off));
            #elif defined(USE_RALLOC)
            void *newNodePtr = ops.newNodeOid;
            #endif
            sl->insert(ops.key, newNodePtr);
            opsPtr->op = OpStruct::done;
        } else if (ops.op == OpStruct::remove) {
            sl->remove(ops.key, ops.oldNodePtr);
            opsPtr->op = OpStruct::done;
            flushToNVM((char *)&ops, sizeof(OpStruct));
            if (workerThreadId == 0) {
                std::pair<uint64_t, void *> removePair;
                removePair.first = logDoneCount;
                removePair.second = ops.oldNodePtr;
                freeQueue->push(removePair);
                ret = true;
            }
        } else {
            // IMPORTANT: I commented this out because it was firing! 
            // The SFU folks have also commented this out in their version.
            
            // if (ops.op == OpStruct::done) { printf("done? %p\n", opsPtr); }
            // exit(1);
        }
        flushToNVM((char *)opsPtr, sizeof(OpStruct));
        smp_wmb();
    }
    workQueue->pop();
    logDoneCount++;
    return ret;
}

bool WorkerThread::isWorkQueueEmpty() { return !workQueue->read_available(); }

void WorkerThread::freeListNodes(uint64_t removeCount) {
    assert(workerThreadId == 0 && freeQueue != NULL);
    if (freeQueue->empty()) return;
    while (!freeQueue->empty()) {
        std::pair<uint64_t, void *> removePair = freeQueue->front();
        if (removePair.first < removeCount) {
            #if defined(USE_PMDK)
            PMEMoid ptr = pmemobj_oid(removePair.second);
            pmemobj_free(&ptr);
            #elif defined(USE_RALLOC)
            PMem::free(removePair.second);
            #endif
            freeQueue->pop();
        } else
            break;
    }
}