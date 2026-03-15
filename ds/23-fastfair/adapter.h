#ifndef P_BWTREE_ADAPTER_H
#define P_BWTREE_ADAPTER_H

#include <iostream>
// #define FAST_FAIR_NO_OPTIMIZE 1

#ifdef FAST_FAIR_NO_OPTIMIZE
#include "btree.h"
#else 
#include "btree-optim.h"
#endif 

#if defined(USE_RALLOC)
#include "ralloc_n0dl.hpp"
#include "ralloc_n1dl.hpp"
#endif

#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif
#ifndef VALUE_TYPE
#define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
#else
#define KEY_TO_VALUE(key) key
#endif


#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T fastfair::btree

template <typename K, typename V, class Reclaim = reclaimer_debra<K>,
          class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T *ds;

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";
        std::cout << "YES_VALUE in adapter: " << YES_VALUE << "\n";
            fastfair::setbench_tid = 0;
        #if defined(USE_RALLOC)
        RP_N0DL_init("ralloc0", 256ull * 1024 * 1024 * 1024);
        RP_N1DL_init("ralloc1", 256ull * 1024 * 1024 * 1024);
        #endif

        ds = new DATA_STRUCTURE_T();

    }

    V getNoValue() {
        //        return nullptr;
        return NO_VALUE;
    }

    void initThread(const int tid) {
        fastfair::setbench_tid = tid;
    }

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {}

    V insertIfAbsent(const int tid, K &key, const V &val) {
        // auto t = ds->getThreadInfo();
        // bool ret = ds->putRetBool(key, KEY_TO_VALUE(key), t);
        auto ret = ds->btree_insert(key, (char *)KEY_TO_VALUE(key));
        return ret ? NO_VALUE : YES_VALUE;
    }

    V erase(const int tid, K &key) {
        // auto t = ds->getThreadInfo();
        // bool ret = ds->delRetBool(key, t);
        auto ret = ds->btree_delete(key);
        return ret ? YES_VALUE : NO_VALUE;
    }

    V find(const int tid, const K &key) {
        // auto t = ds->getThreadInfo();
        // void *ret = ds->get(key, t);
        // if (ret == NULL)
            // return NO_VALUE;
        // else
            // return YES_VALUE;
        auto ret = ds->btree_search(key);
        if (ret == NULL)
            return NO_VALUE;
        else
            return YES_VALUE;
    }

    bool contains(const int tid, const K &key) {
        // auto t = ds->getThreadInfo();
        // void *ret = ds->get(key, t);
        // return (ret != NULL);
        auto ret = ds->btree_search(key);
        return (ret != NULL);
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
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
        using NodePtrType = void*;
        K minKey;
        K maxKey;

        NodeHandler(const K &minKey, const K &maxKey)
            : minKey(minKey), maxKey(maxKey) {}

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

        static bool isLeaf(NodePtrType node) { return true; }

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

        static size_t getSizeInBytes(NodePtrType node) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
                                                return nullptr;
    }

#endif

};

#endif