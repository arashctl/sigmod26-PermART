#ifndef DPTREE_ADAPTER_H
#define DPTREE_ADAPTER_H

#include <iostream>
#include "errors.h"
#include "record_manager.h"
#ifdef USE_TREE_STATS
#   include "tree_stats.h"
#endif

#include "include/concur_dptree.hpp"

#define DATA_STRUCTURE_T dptree::concur_dptree<K, V>


template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    const V NO_VALUE;
    DATA_STRUCTURE_T *index;
public:
    ds_adapter(const int NUM_THREADS,
               const K& unused1,
               const K& unused2,
               const V& unused3,
               Random64 * const unused4)
    : NO_VALUE(unused3)
    {
        index = new DATA_STRUCTURE_T();



        // exit(0);
    }
    ~ds_adapter() {
    }

    V getNoValue() {
        return NO_VALUE;
    }

    void initThread(const int tid) {
        // recmgr->initThread(tid);
    }
    void deinitThread(const int tid) {
        // recmgr->deinitThread(tid);
    }

    bool contains(const int tid, const K& key) {
        return false;
    }
    V insert(const int tid, const K& key, const V& val) {
        setbench_error("not implemented");
        // return NO_VALUE; // succeed
    }
    V insertIfAbsent(const int tid, const K& key, const V& val) {
        index->insert(key, key);
        // return NO_VALUE; // succeed
    }
    V erase(const int tid, const K& key) {
        // doesn't support erase...
        // setbench_error("not implemented");
        index->upsert(key, key*2);
        // return NO_VALUE; // fail
    }
    V find(const int tid, const K& key) {
        return NO_VALUE;
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        return 0;
    }
    void printSummary() {
        // recmgr->printStatus();
    }
    bool validateStructure() {
        return true;
    }
    void printObjectSizes() {
    }
    void debugGCSingleThreaded() {}

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef int * NodePtrType;

        NodeHandler(const K& _minKey, const K& _maxKey) {}

        class ChildIterator {
        public:
            ChildIterator(NodePtrType _node) {}
            bool hasNext() {
                return false;
            }
            NodePtrType next() {
                return NULL;
            }
        };

        bool isLeaf(NodePtrType node) {
            return false;
        }
        size_t getNumChildren(NodePtrType node) {
            return 0;
        }
        size_t getNumKeys(NodePtrType node) {
            return 0;
        }
        size_t getSumOfKeys(NodePtrType node) {
            return 0;
        }
        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), NULL, true);
    }
#endif
};

#endif
