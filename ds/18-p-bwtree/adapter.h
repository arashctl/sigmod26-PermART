#ifndef P_BWTREE_ADAPTER_H
#define P_BWTREE_ADAPTER_H

#include <iostream>

#include "src/bwtree.h"
#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif
#ifndef VALUE_TYPE
#define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
#else
#define KEY_TO_VALUE(key) key
#endif
using namespace wangziqi2013::bwtree;

/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
   public:
    inline bool operator()(const long int k1, const long int k2) const {
        return k1 < k2;
    }

    inline bool operator()(const uint64_t k1, const uint64_t k2) const {
        return k1 < k2;
    }

    inline bool operator()(const char *k1, const char *k2) const {
        return memcmp(k1, k2,
                      strlen(k1) > strlen(k2) ? strlen(k1) : strlen(k2)) < 0;
    }

    KeyComparator(int dummy) {
        (void)dummy;

        return;
    }

    KeyComparator() = delete;
    // KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
   public:
    inline bool operator()(const long int k1, const long int k2) const {
        return k1 == k2;
    }

    inline bool operator()(uint64_t k1, uint64_t k2) const { return k1 == k2; }

    inline bool operator()(const char *k1, const char *k2) const {
        if (strlen(k1) != strlen(k2))
            return false;
        else
            return memcmp(k1, k2, strlen(k1)) == 0;
    }

    KeyEqualityChecker(int dummy) {
        (void)dummy;

        return;
    }

    KeyEqualityChecker() = delete;
    // KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};

#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T BwTree<K, V, KeyComparator, KeyEqualityChecker>

template <typename K, typename V, class Reclaim = reclaimer_debra<K>,
          class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
    struct PaddedValset {
        std::vector<V> __thr_valset;
        char padding[128 - sizeof(std::vector<V>)];
    };

   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T *ds;
    PAD;
    PaddedValset *valsets;

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";
        std::cout << "YES_VALUE in adapter: " << YES_VALUE << "\n";

        ds =
            new DATA_STRUCTURE_T(true, KeyComparator{1}, KeyEqualityChecker{1});
        ds->UpdateThreadLocal(NUM_THREADS);

        valsets = new PaddedValset[NUM_THREADS];
        ds->AssignGCID(0);  // dummy TID for main thread
    }

    V getNoValue() {
        //        return nullptr;
        return NO_VALUE;
    }

    void initThread(const int tid) {
        ds->AssignGCID(tid);
        valsets[tid].__thr_valset.reserve(100);
    }

    void deinitThread(const int tid) { ds->UnregisterThread(tid); }

    V insert(const int tid, const K &key, const V &val) {}

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        bool success = ds->Insert(key, val);
        if (success) { return NO_VALUE; }
        return YES_VALUE;
    }

    V erase(const int tid, K &key) {
        // The bwtree delete function expects the value too.
        bool success = ds->Delete(key, KEY_TO_VALUE(key));
        if (success) { return YES_VALUE; }
        return NO_VALUE;
    }

    V find(const int tid, const K &key) {
        ds->GetValue(key, valsets[tid].__thr_valset);
        // cout << "Size: " << valsets[tid].__thr_valset.size() << endl;
        V retval = valsets[tid].__thr_valset[0];
        valsets[tid].__thr_valset.pop_back();
        return retval;
    }

    bool contains(const int tid, const K &key) {
        ds->GetValue(key, valsets[tid].__thr_valset);
        bool ret = valsets[tid].__thr_valset.size() > 0;
        valsets[tid].__thr_valset.clear();
        return ret;
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {}

    void printSummary() { return; }

    bool validateStructure() { return true; }

    void printObjectSizes() { return; }

    // try to clean up: must only be called by a single thread as part of the
    // test harness!
    void debugGCSingleThreaded() { return; }

#ifdef USE_TREE_STATS
class NodeHandler {
    public:
        typedef NodeHandler * NodePtrType;
        size_t num_keys;
        size_t sum_of_keys;
        NodeHandler(DATA_STRUCTURE_T * ds) {
            printf("Open BwTree: createTreeStats iterating over all kv-pairs...\n");
            num_keys = 0;
            sum_of_keys = 0;
            auto it = ds->Begin();
            for ( ;!it.IsEnd(); it++) {
                ++num_keys;
                sum_of_keys += (size_t) it->first;
            }
            printf("Open BwTree: createTreeStats finished iterating.\n");
        }
        class ChildIterator {
        public:
            ChildIterator(NodeHandler * unused) {}
            bool hasNext() { return false; }
            NodeHandler * next() { return NULL; }
        };
        static bool isLeaf(NodeHandler * unused) { return true; }
        static ChildIterator getChildIterator(NodeHandler * handler) { return ChildIterator(handler); }
        static size_t getNumChildren(NodeHandler * unused) { return 0; }
        static size_t getNumKeys(NodeHandler * handler) { return handler->num_keys; }
        static size_t getSumOfKeys(NodeHandler * handler) { return handler->sum_of_keys; }
        static size_t getSizeInBytes(NodeHandler * unused) { return 0; }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        NodeHandler * handler = new NodeHandler(ds);
        return new TreeStats<NodeHandler>(handler, handler, false);
    }

#endif
};

#endif