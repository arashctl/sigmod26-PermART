// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include <ordo_clock.h>
#include <zconf.h>

#include <cassert>
#include <iostream>

#include "Combiner.h"
#include "WorkerThread.h"
#include "numa-config.h"
#include "numa.h"
#include "pactreeImpl.h"

#ifdef PACTREE_ENABLE_STATS
#define acc_sl_time(x) (curThreadData->sltime += x)
#define acc_dl_time(x) (curThreadData->dltime += x)
#define hydralist_reset_timers() \
    total_sl_time = 0;           \
    total_dl_time = 0;
#define hydralist_start_timer()      \
    {                                \
        unsigned long __b_e_g_i_n__; \
        __b_e_g_i_n__ = read_tsc();
#define hydralist_stop_timer(tick)        \
    (tick) += read_tsc() - __b_e_g_i_n__; \
    }
#else
#define acc_sl_time(x)
#define acc_dl_time(x)
#define hydralist_start_timer()
#define hydralist_stop_timer(tick)
#define hydralist_reset_timers()
#endif

std::vector<WorkerThread *> g_WorkerThreadInst(
    MAX_NUMA *WORKER_THREAD_PER_NUMA);
std::vector<SearchLayer *> g_perNumaSlPtr(MAX_NUMA);
std::set<ThreadData *> g_threadDataSet;
std::atomic<bool> g_globalStop;
std::atomic<bool> g_combinerStop;
thread_local int pactreeImpl::threadNumaNode = -1;
thread_local ThreadData *curThreadData = NULL;
int pactreeImpl::totalNumaActive = 0;
volatile bool threadInitialized[MAX_NUMA];
volatile bool slReady[MAX_NUMA];
std::mutex g_threadDataLock;
uint64_t g_removeCount;
volatile std::atomic<bool> g_removeDetected;

void *PMem::baseAddresses[] = {};
void *PMem::logVaddr[] = {};

void workerThreadExec(int threadId, int activeNuma, root_obj *root) {
    WorkerThread wt(threadId, activeNuma);
    g_WorkerThreadInst[threadId] = &wt;
    if (threadId < activeNuma) {
        while (threadInitialized[threadId] == 0)
            ;
        slReady[threadId] = false;
        //        assert(numa_node_of_cpu(sched_getcpu()) == threadId);
        g_perNumaSlPtr[threadId] =
            pactreeImpl::createSearchLayer(root, threadId);
        g_perNumaSlPtr[threadId]->setNuma(threadId);
        slReady[threadId] = true;
    }
    int count = 0;
    uint64_t lastRemoveCount = 0;
    while (!g_combinerStop) {
        usleep(200);
        while (!wt.isWorkQueueEmpty()) {
            count++;
            wt.applyOperation();
        }
        if (threadId == 0 && lastRemoveCount != g_removeCount) {
            //            wt.freeListNodes(g_removeCount);
        }
    }
    while (!wt.isWorkQueueEmpty()) {
        count++;
        if (wt.applyOperation() && !g_removeDetected) {
            g_removeDetected.store(true);
        }
    }
    // If worker threads are stopping that means there are no more
    // user threads
    if (threadId == 0) { wt.freeListNodes(ULLONG_MAX); }
    g_WorkerThreadInst[threadId] = NULL;

    printf("Worker thread: %d Ops: %d\n", threadId, wt.opcount);
}

uint64_t gracePeriodInit(std::vector<ThreadData *> &threadsToWait) {
    uint64_t curTime = ordo_get_clock();
    for (auto td : g_threadDataSet) {
        if (td->getFinish()) {
            g_threadDataLock.lock();
            g_threadDataSet.erase(td);
            g_threadDataLock.unlock();
            free(td);
            continue;
        }
        if (td->getRunCnt() % 2) {
            if (ordo_gt_clock(td->getLocalClock(), curTime))
                continue;
            else
                threadsToWait.push_back(td);
        }
    }
    return curTime;
}
void waitForThreads(std::vector<ThreadData *> &threadsToWait,
                    uint64_t gpStartTime) {
    for (int i = 0; i < threadsToWait.size(); i++) {
        if (threadsToWait[i] == NULL) continue;
        ThreadData *td = threadsToWait[i];
        if (td->getRunCnt() % 2 == 0) continue;
        if (ordo_gt_clock(td->getLocalClock(), gpStartTime)) continue;
        while (td->getRunCnt() % 2) {
            usleep(1);
            std::atomic_thread_fence(std::memory_order::memory_order_acq_rel);
        }
    }
}
void broadcastDoneCount(uint64_t removeCount) { g_removeCount = removeCount; }
void combinerThreadExec(int activeNuma) {
    CombinerThread ct;
    int count = 0;
    while (!g_globalStop) {
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeNuma);
        }
        uint64_t doneCountWt = ct.freeMergedLogs(activeNuma, false);
        std::vector<ThreadData *> threadsToWait;
        if (g_removeDetected && doneCountWt != 0) {
            uint64_t gpStartTime = gracePeriodInit(threadsToWait);
            waitForThreads(threadsToWait, gpStartTime);
            broadcastDoneCount(doneCountWt);
            g_removeDetected.store(false);
        } else
            usleep(100);
    }
    // TODO fix this
    int i = 20;
    while (i--) {
        usleep(200);
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeNuma);
        }
    }
    while (!ct.mergedLogsToBeFreed()) ct.freeMergedLogs(activeNuma, true);
    g_combinerStop = true;
    printf("Combiner thread: Ops: %d\n", count);
}

void pinThread(std::thread *t, int numaId) {
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    for (int i = 0; i < NUM_SOCKET; i++) {
        for (int j = 0; j < SMT_LEVEL; j++) {
            CPU_SET(OS_CPU_ID[numaId][i][j], &cpuSet);
        }
    }
    int rc =
        pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuSet);
    assert(rc == 0);
}
int cnt = 0;
void pactreeImpl::createWorkerThread(int numNuma, root_obj *root) {
    for (int i = 0; i < numNuma * WORKER_THREAD_PER_NUMA; i++) {
        threadInitialized[i % numNuma] = false;
        std::thread *wt = new std::thread(workerThreadExec, i, numNuma, root);
        usleep(1);
        wtArray->push_back(wt);
        pinThread(wt, i % numNuma);
        threadInitialized[i % numNuma] = true;
    }
}

void pactreeImpl::createCombinerThread() {
    combinerThead = new std::thread(combinerThreadExec, totalNumaActive);
}

std::pair<std::string, std::string> getPMemPaths() {
    auto socket0 = getenv("PACTREE_SOCKET0_ROOT");
    auto socket1 = getenv("PACTREE_SOCKET1_ROOT");

    if (socket0 == NULL || socket1 == NULL) {
        std::cout << "Please set PACTREE_SOCKET0_ROOT and PACTREE_SOCKET1_ROOT"
                  << std::endl;
        exit(1);
    }

#ifdef PREFER_SOCKET0
    return std::make_pair(socket0, socket1);
#else
    return std::make_pair(socket1, socket0);
#endif
}

#if defined(USE_PMDK)
pactreeImpl *initPT(int numa) {
    auto paths = getPMemPaths();
    auto firstRoot = paths.first;
    auto secondRoot = paths.second;

    std::string firstDlpath = firstRoot;
    firstDlpath.append("/dl");

    size_t sz = 35UL * 1024UL * 1024UL * 1024UL;  // 35GB
    int isCreated = 0;
    int isCreated2 = 0;
    root_obj *root = nullptr;
    root_obj *sl_root = nullptr;

    std::string firstSlpath = firstRoot;
    firstSlpath.append("/sl");
    const char *sl_path = firstSlpath.c_str();

    size_t sl_size = 35UL * 1024UL * 1024UL * 1024UL;  // 35GB

    PMem::bind(0, sl_path, sl_size, (void **)&sl_root, &isCreated); // pool 0: sl, 35gb
    if (isCreated == 0) {
        printf("Reading Search layer from an existing pactree.\n");
        assert(false);
    }
    std::string firstLogpath = firstRoot;
    firstLogpath.append("/log");
    const char *log_path = firstLogpath.c_str();

    PMem::bindLog(0, log_path, sz); // log pool 0 -> pool 3: log, 35gb

    PMem::bind(1, firstDlpath.c_str(), sz, (void **)&root, &isCreated2); // pool 1: dl, 35gb

#ifdef MULTIPOOL

    std::string secondDlpath = secondRoot;
    secondDlpath.append("/dl");

    std::string secondSlpath = secondRoot;
    secondSlpath.append("/sl");

    std::string secondLogpath = secondRoot;
    secondLogpath.append("/log");

    const char *path2 = secondDlpath.c_str();
    const char *sl_path2 = secondSlpath.c_str();
    const char *log_path2 = secondLogpath.c_str();
    root_obj *root2 = nullptr;
    root_obj *sl_root2 = nullptr;
    PMem::bind(3, sl_path2, sl_size, (void **)&sl_root2, &isCreated);
    PMem::bind(4, path2, sz, (void **)&root2, &isCreated);
    PMem::bindLog(1, log_path2, sz);
#endif
    if (isCreated2 == 0) {
        // Recovery codepath. commented out for easier migration to ralloc.
        assert(false);
        // pactreeImpl *pt = (pactreeImpl *)pmemobj_direct(root->ptr[0]);
        // pt->init(numa, sl_root);
        // return pt;
    }
    PMEMobjpool *pop = (PMEMobjpool *)PMem::getBaseOf(1);

    int ret =
        pmemobj_alloc(pop, &(root->ptr[0]), sizeof(pactreeImpl), 0, NULL, NULL);
    void *rootVaddr = pmemobj_direct(root->ptr[0]);
    pactreeImpl *pt = (pactreeImpl *)new (rootVaddr) pactreeImpl(numa, sl_root);
    flushToNVM((char *)root, sizeof(root_obj));
    smp_wmb();
    return pt;
}

#elif defined(USE_RALLOC)

pactreeImpl *initPT(int numa) {

    auto opLogSize = sizeof(OpStruct); 
    assert(opLogSize == 64); // Make sure OpStruct size is 64 bytes, as expected by the code


    size_t slSz = 192ULL * 1024 * 1024 * 1024;  // 128GB
    size_t dlSz = 192ULL * 1024 * 1024 * 1024; // 128GB
    size_t logSize = 128ULL * 1024 * 1024 * 1024; // 128GB chunk for logs

    // This has to be smaller than logSize because some of logSize is used for metadata by ralloc. 
    size_t logSizeToAllocate = 100ULL * 1024 * 1024 * 1024; // 10GB chunk for logs

    if (numa == 1) {
        RP_N0DL_init("numa0dl", dlSz);
        RP_N0SL_init("numa0sl", slSz);
        RP_N0LOG_init("numa0log", logSize);
    } else if (numa == 2) {
        std::cout << "Initing n1log with " << logSize << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N1LOG_init("numa1log", logSize);

        std::cout << "Initing n0log with " << logSize << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N0LOG_init("numa0log", logSize);

        std::cout << "Initing n1dl with " << dlSz << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N1DL_init("numa1dl", dlSz);

        std::cout << "Initing n0dl with " << dlSz << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N0DL_init("numa0dl", dlSz);

        std::cout << "Initing n1sl with " << slSz << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N1SL_init("numa1sl", slSz);

        std::cout << "Initing n0sl with " << slSz << " bytes" << std::endl;
        std::cout << std::flush;
        RP_N0SL_init("numa0sl", slSz);
    } else {
        assert(false);
    }

    // PMem::bindLog allocates a huge chunk of memory for logs when there's PMDK
    // And then, PMem::getOpLog(idx) returns a pointer to the idx-th log entry
    // In ralloc, let's replicate that behavior

    if (numa == 1) {
        PMem::bindLog(0, "ignored", logSizeToAllocate);
    } else if (numa == 2) {
        PMem::bindLog(0, "ignored", logSizeToAllocate);
        PMem::bindLog(1, "ignored", logSizeToAllocate);
    } else {
        assert(false);
    }

    // RALLOC TODO: initialize the tree
    // Allocate it, call the constructor and everything

    // Pool 1 is for data layer
    void *ptr = RP_N0DL_malloc(sizeof(pactreeImpl));
    pactreeImpl *pt = new (ptr) pactreeImpl(numa, nullptr); // We don't care about root obj 

    flushToNVM((char *)ptr, sizeof(pactreeImpl));
    smp_wmb();

    return pt;


    
}

#endif


// Recovery codepath. commented out for easier migration to ralloc.
// void pactreeImpl::init(int numNuma, root_obj *root) {
//     int id = 0;

//     assert(numNuma <= NUM_SOCKET);
//     totalNumaActive = numNuma;
//     wtArray = new std::vector<std::thread *>(totalNumaActive);
//     g_WorkerThreadInst.clear();
//     g_perNumaSlPtr.clear();
//     g_threadDataSet.clear();

//     g_perNumaSlPtr.resize(totalNumaActive);
//     g_globalStop = false;
//     g_combinerStop = false;
//     createWorkerThread(numNuma, root);
//     createCombinerThread();
//     for (int i = 0; i < totalNumaActive; i++) {
//         while (slReady[i] == false)
//             ;
//     }
//     recover();
//     hydralist_reset_timers();
// }


// Common between USE_RALLOC and USE_PMDK.
// There's no explicit allocation logic here. 
// The side effects that we care about are: 
// 1. dl.initialize() 
// 2. createWorkerThread() -> workerThreadExec() -> createSearchLayer()


pactreeImpl::pactreeImpl(int numNuma, root_obj *root) {
    assert(numNuma <= NUM_SOCKET);
    totalNumaActive = numNuma;
    wtArray = new std::vector<std::thread *>(totalNumaActive);
    g_perNumaSlPtr.resize(totalNumaActive);
    dl.initialize();
    // need to read from PM

    g_globalStop = false;
    g_combinerStop = false;
#ifdef SYNC
    totalNumaActive = 1;
    g_perNumaSlPtr.resize(totalNumaActive);
    g_perNumaSlPtr[0] = pactreeImpl::createSearchLayer(root, 0);
#else
    createWorkerThread(numNuma, root);
    createCombinerThread();
    for (int i = 0; i < totalNumaActive; i++) {
        while (slReady[i] == false)
            ;
    }
#endif
    hydralist_reset_timers();
}

pactreeImpl::~pactreeImpl() {
    g_globalStop = true;
    for (auto &t : *wtArray) t->join();
    combinerThead->join();

    std::cout << "Total splits: " << numSplits << std::endl;
    std::cout << "Combiner splits: " << combinerSplits << std::endl;
#ifdef PACTREE_ENABLE_STATS
    std::cout << "total_dl_time: " << total_dl_time / numThreads / 1000
              << std::endl;
    std::cout << "total_sl_time: " << total_sl_time / numThreads / 1000
              << std::endl;
#endif
}

ListNode *pactreeImpl::getJumpNode(Key_t &key) {
    int numaNode = getThreadNuma();
    SearchLayer &sl = *g_perNumaSlPtr[numaNode];
    if (sl.isEmpty()) { return dl.getHead(); }
    ListNode *jumpNode = reinterpret_cast<ListNode *>(sl.lookup(key));
    //    idxPtr2 jumpNode = reinterpret_cast<idxPtr2>(sl.lookup(key));
    if (jumpNode == nullptr) jumpNode = dl.getHead();
    return jumpNode;
}

#ifdef SYNC

// lock interface
ListNode *pactreeImpl::getJumpNodewithLock(Key_t &key, void **node) {
    SearchLayer &sl = *g_perNumaSlPtr[0];
    if (sl.isEmpty()) return dl.getHead();
    auto *jumpNode = reinterpret_cast<ListNode *>(sl.lookupwithLock(key, node));
    if (jumpNode == nullptr) { jumpNode = dl.getHead(); }
    return jumpNode;
}

// unlock interface
bool pactreeImpl::JumpNodewithUnLock(void *node) {
    SearchLayer &sl = *g_perNumaSlPtr[0];

    return sl.nodeUnlock(node);
}
#endif

bool pactreeImpl::insert(Key_t &key, Val_t val) {
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);
    uint64_t ticks = 0;

    int threadId = curThreadData->getThreadId();
    bool ret;
    hydralist_start_timer();
    ListNode *jumpNode;
#ifdef SYNC
    void *art_node = nullptr;
    jumpNode = getJumpNodewithLock(key, &art_node);
#else
    jumpNode = getJumpNode(key);
#endif

    hydralist_stop_timer(ticks);
    hydralist_start_timer();
    acc_sl_time(ticks);
#ifndef SYNC
    ret = dl.insert(key, val, jumpNode, threadId);
#else
    ret = dl.insert(key, val, jumpNode, &art_node);
    if (art_node != nullptr) { JumpNodewithUnLock(art_node); }
#endif

    hydralist_stop_timer(ticks);
    acc_dl_time(ticks);
    curThreadData->read_unlock();

    return ret;
}

bool pactreeImpl::update(Key_t &key, Val_t val) {
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);

    ListNode *jumpNode = getJumpNode(key);

    bool ret = dl.update(key, val, jumpNode);
    curThreadData->read_unlock();
    return ret;
}

Val_t pactreeImpl::lookup(Key_t &key) {
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);
    Val_t val;
    uint64_t ticks;
    // hydralist_start_timer();

    ListNode *jumpNode = getJumpNode(key);
    // hydralist_stop_timer(ticks);
    // acc_sl_time(ticks);

    // hydralist_start_timer();
    //
    dl.lookup(key, val, jumpNode);
    // hydralist_stop_timer(ticks);
    // acc_dl_time(ticks);
    curThreadData->read_unlock();
    return val;
}

bool pactreeImpl::remove(Key_t &key) {
    uint64_t clock = ordo_get_clock();
    curThreadData->read_lock(clock);

    ListNode *jumpNode = getJumpNode(key);

    bool ret = dl.remove(key, jumpNode);
    curThreadData->read_unlock();
    return ret;
}

#if defined(USE_PMDK)
SearchLayer *pactreeImpl::createSearchLayer(root_obj *root, int threadId) {
    if (pmemobj_direct(root->ptr[threadId]) == nullptr) {
        pptr<SearchLayer> sPtr;
        PMem::alloc(0, sizeof(SearchLayer), (void **)&sPtr,
                    &(root->ptr[threadId]));
        SearchLayer *s = new (sPtr.getVaddr()) SearchLayer();
        return s;
    } else {

        // Recovery codepath. commented out for easier migration to ralloc.
        assert(false);
        // SearchLayer *s = (SearchLayer *)pmemobj_direct(root->ptr[threadId]);
        // s->init();
        // return s;
    }

    // Read from Root Object or Allocate new one.
}
#elif defined(USE_RALLOC)
SearchLayer *pactreeImpl::createSearchLayer(root_obj *root, int threadId) {
    // We ignore root_obj with ralloc 
    pptr<SearchLayer> sPtr;
    PMem::alloc(
        0, 
        sizeof(SearchLayer),
        (void **)&sPtr
        // No fourth argument, as we don't use root_obj with ralloc
    );

    SearchLayer *s = new (sPtr.getVaddr()) SearchLayer();
    return s;
}
#endif

unsigned long tacc_rdtscp(int *chip, int *core) {
    unsigned long a, d, c;
    __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
    *chip = (c & 0xFFF000) >> 12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);
    ;
}

int pactreeImpl::getThreadNuma() {
    int chip;
    int core;
    if (threadNumaNode == -1) {
        tacc_rdtscp(&chip, &core);
        threadNumaNode = chip;
        if (threadNumaNode > 8) assert(0);
    }
    if (totalNumaActive <= threadNumaNode)
        return 0;
    else
        return threadNumaNode;
}

uint64_t pactreeImpl::scan(Key_t &startKey, int range,
                           std::vector<Val_t> &result) {
    ListNode *jumpNode = getJumpNode(startKey);

    return dl.scan(startKey, range, result, jumpNode);
}

uint64_t pactreeImpl::rangeQuery(Key_t &startKey, Key_t &endKey,
                                 std::vector<Val_t> &result) {
    ListNode *jumpNode = getJumpNode(startKey);

    return dl.rangeQuery(startKey, endKey, result, jumpNode);
}

uint64_t pactreeImpl::scanThenUnlockAll(Key_t &startKey, int range,
                                        std::vector<Val_t> &result) {
    ListNode *jumpNode = getJumpNode(startKey);

    return dl.scanThenUnlockAll(startKey, range, result, jumpNode);
}

uint64_t pactreeImpl::rangeQueryThenUnlockAll(Key_t &startKey, Key_t &endKey,
                                              std::vector<Val_t> &result) {
    ListNode *jumpNode = getJumpNode(startKey);

    return dl.rangeQueryThenUnlockAll(startKey, endKey, result, jumpNode);
}

void pactreeImpl::registerThread() {
    int threadId = numThreads.fetch_add(1);
    auto td = new ThreadData(threadId);
    g_threadDataLock.lock();
    g_threadDataSet.insert(td);
    g_threadDataLock.unlock();
    curThreadData = td;
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

void pactreeImpl::unregisterThread() {
    if (curThreadData == NULL) return;
    int threadId = curThreadData->getThreadId();
    curThreadData->setfinish();
#ifdef PACTREE_ENABLE_STATS
    total_dl_time.fetch_add(curThreadData->dltime);
    total_sl_time.fetch_add(curThreadData->sltime);
#endif
}


// Recovery codepath. commented out for easier migration to ralloc.
// void pactreeImpl::recover() { 
//     assert(false);    
//     dl.Recovery(g_perNumaSlPtr[0]); 
// }
