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

#include "fptree.h"

#define DATA_STRUCTURE_T FPtree

template <typename K, typename V, class Reclaim = void, class Alloc = void,
          class Pool = void>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T fptree;

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        #ifdef PMEM
        fptree.pmemInit("/mnt/pmem1_mount/mkhalaji/pmemobj_playground/file.dat",
                        48 * 1024 * 1024 * 1024ULL);
        #endif 
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";
    }

    void *getNoValue() { return (void *)NO_VALUE; }

    void initThread(const int tid) {}

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {
        setbench_error(
            "insert-replace functionality not implemented for this data "
            "structure");
    }

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        struct KV kv = {key, key};
        if (fptree.insert(kv))
            return NO_VALUE;
        else
            return val;
    }

    V erase(const int tid, const K &key) {
        if (fptree.deleteKey(key))
            return YES_VALUE;
        else
            return NO_VALUE;
    }

    V find(const int tid, const K &key) { return (V)fptree.find(key); }

    bool contains(const int tid, const K &key) {
        if (fptree.find(key) == 0)
            return false;
        else
            return true;
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
        setbench_error(
            "Range query functionality not implemented for this data "
            "structure");
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
                if (!node) { 
                    return false; }
                if (!node->isInnerNode) { 
                    return false; }
                InnerNode *inner = reinterpret_cast<InnerNode *>(node);
                auto ret = ix <= inner->nKey; // why equal though? got from printFPTree... in fptree.cpp
                return ret;
            }

            NodePtrType next() {
                if (!node) { return nullptr; }
                if (!node->isInnerNode) { return nullptr; }
                InnerNode *inner = reinterpret_cast<InnerNode *>(node);
                return inner->p_children[ix++];
            }
        };

        static bool isLeaf(NodePtrType node) {
            if (!node) { 
                return false; }
            auto x = !node->isInnerNode;
            return x;
        }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) {
            if (!node) { return 0; }
            if (!node->isInnerNode) { return 0; }
            InnerNode *inner = reinterpret_cast<InnerNode *>(node);
            return inner->nKey;
        }

        static size_t getNumKeys(NodePtrType node) {
            if (!node) { 
                return 0; }
            if (node->isInnerNode) { 
                return 0; }
            LeafNode *leaf = reinterpret_cast<LeafNode *>(node);
            size_t res{0};
            for (int i = MAX_LEAF_SIZE - 1; i >= 0; i--) {
                if (leaf->bitmap.test(i)) { res++; }
            }
            return res;
        }

        static size_t getSumOfKeys(NodePtrType node) {
            if (!node) { return 0; }
            if (node->isInnerNode) { return 0; }
            LeafNode *leaf = reinterpret_cast<LeafNode *>(node);
            size_t res{0};
            for (int i = MAX_LEAF_SIZE - 1; i >= 0; i--) {
                if (leaf->bitmap.test(i)) {
                    auto cur = leaf->kv_pairs[i].key;
                    res += cur;
                }
            }
            return res;
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