#ifndef LBTREE_ADAPTER_H
#define LBTREE_ADAPTER_H

#include <immintrin.h>

#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

#include "errors.h"
#include "mempool.h"
#include "tree.h"

#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif

#ifdef USE_RALLOC
#include "ralloc_n0dl.hpp"
#include "ralloc_n1dl.hpp"
#endif

// TODO: include the correct header file
#include "lbtree.h"

// TODO: define the correct data structure type
#define DATA_STRUCTURE_T tree
constexpr const auto MEMPOOL_ALIGNMENT = 4096LL;

template <typename K, typename V, class Reclaim = void, class Alloc = void,
          class Pool = void>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T *lbt;

   public:
    // TODO: add the ds to the constructor
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        initUseful();
        worker_thread_num = NUM_THREADS;
        worker_id = 0;  // MUST be allocated because nvmpool_alloc needs it

        the_thread_mempools.init(NUM_THREADS, 4096, MEMPOOL_ALIGNMENT);
#ifdef USE_RALLOC

        RP_N0DL_init("ralloc0", 256ull * 1024 * 1024 * 1024);
        RP_N1DL_init("ralloc1", 256ull * 1024 * 1024 * 1024);

#else

        auto path_ptr = new std::string(
            "/mnt/pmem1_mount/mkhalaji/lbtree_playground/file.dat");
        // auto path_ptr = new std::string("/tmp/mkhalaji/file.dat");
        const char *pool_path_ = (*path_ptr).c_str();

        size_t pool_size_ = ((size_t)(1024 * 1024 * 32) * 1024);  // 32 GB

#ifdef PMEM
        std::cout << "PMEM Pool Path: " << pool_path_ << std::endl;
        std::cout << "PMEM Pool size: " << pool_size_ << std::endl;
#endif

        auto start = std::chrono::steady_clock::now();
        the_thread_nvmpools.init(NUM_THREADS, pool_path_, pool_size_);
        auto end = std::chrono::steady_clock::now();
        auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "nvmpools init time: " << ms.count() << " ms" << std::endl;

#endif
        char *nvm_addr = (char *)nvmpool_alloc(256);
        // std::cout << "nvm_addr: " << nvm_addr << std::endl;

        // std::cout << "MAX_KEY: " << KEY_MAX << std::endl;

        nvmLogInit(NUM_THREADS);

        lbt = initTree(nvm_addr, false);

        simpleKeyInput input(2, 42069, 1);
        auto root =
            lbt->bulkload(1, &input, 1.0);  // keynum has to be at least 1
        // now delete the meme key:
        lbt->del(42069);

        std::cout << "Root: " << root << std::endl;
    }

    void *getNoValue() { return (void *)NO_VALUE; }

    void initThread(const int tid) {
        worker_id = tid;  // thread_local thread_id must be set for pools
    }

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {
        setbench_error(
            "insert-replace functionality not implemented for this data "
            "structure");
    }

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        // setbench_error(
        //     "insert-replace functionality not implemented for this data "
        //     "structure");
        auto ret = lbt->insert(key, val);
        if (ret) return NO_VALUE;  // insert succeeded
        return YES_VALUE;
    }

    V erase(const int tid, const K &key) {
        // setbench_error(
        //     "insert-replace functionality not implemented for this data "
        //     "structure");
        auto ret = lbt->del(key);
        if (ret) {
            return YES_VALUE;  // delete succeeded
        }
        return NO_VALUE;
    }

    V find(const int tid, const K &key) {
        int pos = -1;
        auto ret = lbt->lookup(key, &pos);
        if (pos >= 0) {
            // lbt->get_recptr(ret, pos);
            return YES_VALUE;
        }
        return NO_VALUE;
        // setbench_error(
        // "insert-replace functionality not implemented for this data "
        // "structure");
    }

    bool contains(const int tid, const K &key) {
        if (find(tid, key) == NO_VALUE) { return false; }
        return true;
        // setbench_error(
        //     "insert-replace functionality not implemented for this data "
        //     "structure");
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
        // char *results = new char[(scanSize + 1) * sizeof(IdxEntry)];
        constexpr size_t ONE_MB = 1ULL << 20;
        static thread_local char results[ONE_MB];

#ifdef CALL_OLD_LBTREE_RANGE_SCAN
        uint32_t scanSize = hi - lo + 1;
        auto ret = lbt->rangeScan(tid, lo, scanSize, results);
#else
        auto ret = lbt->rangeScanLoHi(tid, lo, hi, results);
#endif

        GSTATS_ADD_IX(tid, range_return_size, 1, ret.first);
        GSTATS_ADD_IX(tid, hand_over_hand_range_histogram, 1, ret.second);
        return ret.first;
    }

    void printSummary() { return; }

    bool validateStructure() { return true; }

    void printObjectSizes() { return; }

    // try to clean up: must only be called by a single thread as part of the
    // test harness!
    void debugGCSingleThreaded() { return; }

#ifdef USE_TREE_STATS

    class NodeHandler {
       public:
        using NodePtrType = tree *;

        K minKey;
        K maxKey;

        NodeHandler(const K &_minKey, const K &_maxKey)
            : minKey(_minKey), maxKey(_maxKey) {
            std::cout << "MinKey: " << minKey << std::endl;
            std::cout << "MaxKey: " << maxKey << std::endl;
        }

        class ChildIterator {
           private:
            int ix;
            NodePtrType node;

           public:
            ChildIterator(NodePtrType node) : ix(0), node(node) {}

            bool hasNext() { return false; }

            NodePtrType next() { return nullptr; }
        };

        static bool isLeaf(NodePtrType node) { return false; }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) { return 0; }

        static size_t getNumKeys(NodePtrType node) {
            size_t res = 0;
            for (size_t i = 1; i <= 24000000; i++) {
                int pos = -1;
                node->lookup(i, &pos);
                if (pos >= 0) { res++; }
            }
            return res;
        }

        static size_t getSumOfKeys(NodePtrType node) {
            // return 0;

            size_t res = 0;

            int pos = -1;
            for (size_t i = 1; i <= 24000000; i++) {
                pos = -1;
                node->lookup(i, &pos);
                if (pos >= 0) { res += i; }
            }

            size_t ds_sum = 0;

            auto lbt = (lbtree *)node;
            auto root = lbt->tree_meta->tree_root;
            auto root_level = lbt->tree_meta->root_level;
            std::cout << "Root Level: " << root_level << std::endl;

            return res;
        }

        static size_t getSizeInBytes(NodePtrType nodeF) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
        auto x = new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey),
                                            lbt, false);
        return x;
    }

#endif
};

#endif