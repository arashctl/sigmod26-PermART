#ifndef P_BWTREE_ADAPTER_H
#define P_BWTREE_ADAPTER_H

#include <bits/stdint-uintn.h>
#include <iostream>

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>


#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif
#ifndef VALUE_TYPE
#define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
#else
#define KEY_TO_VALUE(key) key
#endif

typedef struct IntKeyVal {
    uint64_t key;
    uintptr_t value;
} IntKeyVal;

template<typename ValueType = IntKeyVal *>
class IntKeyExtractor {
    public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key;
    }
};



#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor>

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
        ds = new DATA_STRUCTURE_T();
    }

    V getNoValue() {
        //        return nullptr;
        return NO_VALUE;
    }

    void initThread(const int tid) {}

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {}

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        IntKeyVal *kv;
        posix_memalign((void **)&kv, 64, sizeof(IntKeyVal));
        kv->key = (uint64_t) key;
        kv->value = (uintptr_t) val;
        bool success = ds->insert(kv);
        if (success) { return NO_VALUE; }
        return YES_VALUE;
    }

    V erase(const int tid, K &key) {

    }

    V find(const int tid, const K &key) {
    }

    bool contains(const int tid, const K &key) {
        auto result = ds->lookup((uint64_t) key);
        return result.mIsValid;
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