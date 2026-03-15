#ifndef P_PACTREE_SOSP_ADAPTER_H
#define P_PACTREE_SOSP_ADAPTER_H

#include <iostream>

#include "pactree.h"
#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif
#ifndef VALUE_TYPE
#define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
#else
#define KEY_TO_VALUE(key) key
#endif

#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T pactree

template <typename K, typename V, class Reclaim = reclaimer_debra<K>,
          class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T *ptree;

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";
        std::cout << "YES_VALUE in adapter: " << YES_VALUE << "\n";
        // ds = initPT(1);
        ptree = new DATA_STRUCTURE_T(2);
    }

    V getNoValue() {
        //        return nullptr;
        return NO_VALUE;
    }

    void initThread(const int tid) { ptree->registerThread(); }

    void deinitThread(const int tid) { ptree->unregisterThread(); }

    V insert(const int tid, const K &key, const V &val) {}

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        return ptree->insert(key, key) ? NO_VALUE : val;
    }

    V erase(const int tid, K &key) {
        return ptree->remove(key) ? YES_VALUE : NO_VALUE;
    }

    V find(const int tid, const K &key) {
        auto val = ptree->lookup(key);
        if (val == key) {
            // ONLY BECAUSE I'M INSERTING (KEY, KEY) PAIRS
            return val;
        }
        return NO_VALUE;
    }

    bool contains(const int tid, const K &key) {
        auto val = ptree->lookup(key);
        return val == key;
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
        // setbench_error(
        //     "Range query functionality not implemented for this data "
        //     "structure");
        


        std::vector<K> *res = new std::vector<K>();
        // auto ret = ptree->rangeQuery(lo, hi, *res);

        #ifndef PACTree_Linearizable_RangeQuery
        auto ret = ptree->rangeQuery(lo, hi, *res);
        #else
        auto ret = ptree->rangeQueryThenUnlockAll(lo, hi, *res);
        #endif
        // std::string s = "";
        // for (auto r : *res) {
        //     s += std::to_string(r) + ", ";
        // }
        // COUTATOMIC("Range query result for " << lo << " to " << hi << ": " << s);

        return ret; 

        // int range = hi - lo + 1;
        // std::vector<K> result(range);
        // #ifndef ORIGINAL_RANGE_QUERY
        // return ptree->scanThenUnlockAll(lo, range, result);
        // #else
        // return ptree->scan(lo, range, result);
        // #endif
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
        using NodePtrType = ListNode *;

        K minKey;
        K maxKey;

        NodeHandler(const K &_minKey, const K &_maxKey)
            : minKey(_minKey), maxKey(_maxKey) {
            printf("Creating NodeHandler\n");
            printf("minKey: %d\n", minKey);
            printf("maxKey: %d\n", maxKey);
        }

        class ChildIterator {
           private:
            NodePtrType node;

           public:
            ChildIterator(NodePtrType node) : node(node) {}

            bool hasNext() {
                return false;
                // if (node == nullptr) {
                //     return false;
                // }

                // auto next = node->getNext();

                // if (next == nullptr) {
                //     return false;
                // }

                // if (next->getNext() == nullptr) {
                //     return false; // sentinel node is at last
                // }

                // return true;

                // // return node != nullptr && node->getNext() != nullptr;
            }

            NodePtrType next() {
                return nullptr;
                // auto ret = node->getNext();
                // return ret;
            }
        };

        static bool isLeaf(NodePtrType node) {
            return node->getNext() == nullptr;
        }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) { return 0; }

        static size_t getNumKeys(NodePtrType node) {
            ListNode *cur = node;
            size_t ret = 0;
            while (cur->getNext() != nullptr) {
                // printf("NUM: Seeing %d entries at %p\n",
                // cur->getNumEntries(), cur); printf("NUM: next is %p\n",
                // cur->getNext()); printf("NUM: Is my next null? %d\n",
                // cur->getNext() == nullptr);
                // printf("-----------------------------------------------------------------------\n");
                // fflush(stdout);
                if (cur->getNext() == nullptr) { break; }
                ret += cur->getNumEntries();
                cur = cur->getNext();
            }
            return ret;
        }

        static size_t getSumOfKeys(NodePtrType node) {
            // printf("Getting sum for node %p\n", node);
            // fflush(stdout);
            //     for (int i = 0; i < MAX_ENTRIES; i++) {
            //         if (node->bitMap[i]) {
            //             sum += node->keyArray[i].first;
            //         }
            //     }
            //     return sum;
            size_t sum = 0;
            ListNode *cur = node;
            while (cur->getNext() != nullptr) {
                // printf("SUM: Seeing %d entries at %p\n",
                // cur->getNumEntries(), cur); fflush(stdout);
                if (cur->getNext() == nullptr) {
                    break;  // no idea why... going crazy
                }
                for (int i = 0; i < MAX_ENTRIES; i++) {
                    if (cur->bitMap[i]) { sum += cur->keyArray[i].first; }
                }
                cur = cur->getNext();
            }
            return sum;
        }

        static size_t getSizeInBytes(NodePtrType nodeF) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
        // printf("Creating TreeStats\n");
        auto head = ptree->pt->dl.headPtr.getVaddr();
        // ptree->pt->dl.print(head);

        // exit(0);

        auto x = new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey),
                                            (ListNode *)head, false);
        return x;
    }

#endif
};

#endif