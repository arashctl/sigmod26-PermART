#ifndef FPTREE_ADAPTER_H
#define FPTREE_ADAPTER_H

#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>

#include "errors.h"

#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif

// TODO: include the correct header file
#include "tree_api.hpp"
#include "Tree.h"
#include "threadinfo.h"
#include <sys/types.h>
#include <algorithm>
#include <atomic>

// using namespace PART_ns;


// TODO: define the correct data structure type
#define DATA_STRUCTURE_T PART_ns::Tree

template <typename K, typename V, class Reclaim = void, class Alloc = void,
          class Pool = void>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T roart;

   public:
   // TODO: add the ds to the constructor
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
    }

    void *getNoValue() { return (void *)NO_VALUE; }

    void initThread(const int tid) {
        // For ralloc to use 
        NVMMgr_ns::setbench_tid = tid;
        NVMMgr_ns::register_threadinfo();
    }

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {
        setbench_error(
            "insert-replace functionality not implemented for this data "
            "structure");
    }

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        PART_ns::Key k = PART_ns::Key(*reinterpret_cast<const uint64_t*>(&key), sizeof(K), *reinterpret_cast<const uint64_t*>(&val));
        PART_ns::Tree::OperationResults result = roart.insert(&k);
        if (result == PART_ns::Tree::OperationResults::Success) {
            return NO_VALUE;
        } else {
            return val;
        }
    }

    V erase(const int tid, const K &key) {
        PART_ns::Key k = PART_ns::Key(*reinterpret_cast<const uint64_t*>(&key), sizeof(K), 0);
        PART_ns::Tree::OperationResults result = roart.remove(&k);
        if (result == PART_ns::Tree::OperationResults::Success) {
            return YES_VALUE;
        } else {
            return NO_VALUE;
        }
    }

    V find(const int tid, const K &key) { 
        PART_ns::Key k = PART_ns::Key(*reinterpret_cast<const uint64_t*>(&key), sizeof(K), 0);
        auto leaf = roart.lookup(&k);
        if (leaf != nullptr) {
            // TODO: return the correct value
            return YES_VALUE;
        } else {
            return NO_VALUE;
        }
     }

    bool contains(const int tid, const K &key) {
        if (this->find(tid, key) == NO_VALUE) {
            return false;
        } else {
            return true;
        }
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
        
        size_t resultCount = 0;
        constexpr size_t ONE_MB = 1ULL << 20;
        static thread_local char results[ONE_MB];

        PART_ns::Key kstart = PART_ns::Key(*reinterpret_cast<const uint64_t*>(&lo), sizeof(K), 0);
        PART_ns::Key kend = PART_ns::Key(*reinterpret_cast<const uint64_t*>(&hi), sizeof(K), 0);


        // bool lookupRange(const Key *start, const Key *end, const Key *continueKey,
        //              Leaf *result[], std::size_t resultLen,
        //              std::size_t &resultCount) const;

        size_t resultLen = hi - lo + 1;
        // printf("ResultLen: %lu\n", resultLen);
        // printf("Lo: %lu\n", lo);
        // printf("Hi: %lu\n", hi);
        // printf("ResultCount: %lu\n", resultCount);
        roart.lookupRange(&kstart, &kend, nullptr, (PART_ns::Leaf**)&results, resultLen, resultCount);


        return resultCount;

        // setbench_error(
        //     "Range query functionality not implemented for this data "
        //     "structure");
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
        using NodePtrType = BaseNode *;

        K minKey;
        K maxKey;

        NodeHandler(const K &_minKey, const K &_maxKey)
            : minKey(_minKey), maxKey(_maxKey) {}

        class ChildIterator {
           private:
            int ix;
            NodePtrType node;

           public:
            ChildIterator(NodePtrType node) : ix(0), node(node) {}

            bool hasNext() {
                return false;
            }

            NodePtrType next() {
                return nullptr;
            }
        };

        static bool isLeaf(NodePtrType node) {
            return false;
        }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) {
            return 0;
        }

        static size_t getNumKeys(NodePtrType node) {
            return 0;
        }

        static size_t getSumOfKeys(NodePtrType node) {
            return 0;
        }

        static size_t getSizeInBytes(NodePtrType nodeF) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
        auto x = new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey),
                                          fptree.getRoot(), false);
        return x;
    }

#endif
};

#endif