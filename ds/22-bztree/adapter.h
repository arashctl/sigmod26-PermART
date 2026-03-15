#ifndef P_BZTREE_ADAPTER_H
#define P_BZTREE_ADAPTER_H

#include <iostream>
#include <memory>

#include "bztree.h"
#ifdef USE_TREE_STATS
#include "tree_stats.h"
#endif

#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T bztree::BzTree

size_t globalSumOfKeys = 0;

#ifdef PMDK
std::string get_pmem_path() {
    const char *pmem_path = std::getenv("BZTREE_PMEM_PATH");
    if (pmem_path == nullptr) {
        return "/mnt/pmem1_mount/mkhalaji/bztree_playground/file.dat";
    }
    return std::string(pmem_path);
}

auto get_pmem_size() {
    const char *pmem_size_str = std::getenv("BZTREE_PMEM_SIZE");
    if (pmem_size_str == nullptr) {
        return 10ull * 1024 * 1024 * 1024;  // 45 GB
    }
    return std::stoull(pmem_size_str);
}
#endif

static constexpr int kDescriptorsPerThread = 1024;

template <typename K, typename V, class Reclaim = reclaimer_debra<K>,
          class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T *bztree;

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE) {
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";
        std::cout << "YES_VALUE in adapter: " << YES_VALUE << "\n";
        // ds = initPT(1);
        initBzTree(NUM_THREADS);
    }

    void initBzTree(const int NUM_THREADS) {
        uint32_t num_threads = NUM_THREADS;
        uint32_t desc_pool_size = kDescriptorsPerThread * num_threads;
        bztree::BzTree::ParameterSet param(1024, 512, 1024);

#ifdef PMDK
        pmwcas::InitLibrary(
            pmwcas::PMDKAllocator::Create(get_pmem_path().c_str(),
                                          "bztree_layout", get_pmem_size()),
            pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
            pmwcas::LinuxEnvironment::Destroy);

        auto pmdk_allocator =
            reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
        bztree::Allocator::Init(pmdk_allocator);

        bztree = reinterpret_cast<bztree::BzTree *>(
            pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
        pmdk_allocator->Allocate((void **)&bztree->pmwcas_pool,
                                 sizeof(pmwcas::DescriptorPool));
        new (bztree->pmwcas_pool)
            pmwcas::DescriptorPool(desc_pool_size, num_threads, false);

        new (bztree) bztree::BzTree(
            param, bztree->pmwcas_pool,
            reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));
#else
        pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                            pmwcas::DefaultAllocator::Destroy,
                            pmwcas::LinuxEnvironment::Create,
                            pmwcas::LinuxEnvironment::Destroy);
        auto pool = new pmwcas::DescriptorPool(2000, num_threads, false);
        bztree = bztree::BzTree::New(param, pool);
#endif
    }

    V getNoValue() {
        //        return nullptr;
        return NO_VALUE;
    }

    void initThread(const int tid) {}

    void deinitThread(const int tid) {}

    V insert(const int tid, const K &key, const V &val) {}

    V insertIfAbsent(const int tid, const K &key, const V &val) {
        // return ptree->insert(key, key) ? NO_VALUE : val;

        std::string key_str = std::to_string(key);
        auto res = bztree->Insert(key_str.c_str(), key_str.size(), key);
        return res.IsOk() ? NO_VALUE : val;
    }

    V erase(const int tid, K &key) {
        // return ptree->remove(key) ? YES_VALUE : NO_VALUE;

        std::string key_str = std::to_string(key);
        auto res = bztree->Delete(key_str.c_str(), key_str.size());
        return res.IsOk() ? YES_VALUE : NO_VALUE;
    }

    V find(const int tid, const K &key) {
        std::string key_str = std::to_string(key);
        uint64_t value;
        auto res = bztree->Read(key_str.c_str(), key_str.size(), &value);
        return res.IsOk() ? (V)value : NO_VALUE;
    }

    bool contains(const int tid, const K &key) {
        std::string key_str = std::to_string(key);
        uint64_t value;
        auto res = bztree->Read(key_str.c_str(), key_str.size(), &value);
        return res.IsOk();
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
        using NodePtrType = bztree::Iterator*;

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
            return false;
        }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) { return 0; }

        static size_t getNumKeys(NodePtrType node) { 
            size_t ret = 0; 
            size_t _sum = 0;
            while (true) {
                auto record = node->GetNext(); 
                if (record == nullptr) break;
                ret++;
                _sum += std::stoull(std::string(record->GetKey(), record->GetKey() + record->meta.GetKeyLength()));
            }

            // Context (bad code)
            //  getNumKeys is called before getSumOfKeys, so we can store the sum here
            globalSumOfKeys = _sum;
            return ret;
        }

        static size_t getSumOfKeys(NodePtrType node) { 
            return globalSumOfKeys;
        }

        static size_t getSizeInBytes(NodePtrType nodeF) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
        // auto iterator = bztree->RangeScanBySize("1", 1, 400000000);
        // bztree::Iterator *unsafePtr = iterator.get();
        // return new TreeStats<NodeHandler>(
            // new NodeHandler(_minKey, _maxKey), unsafePtr, true);
        return nullptr;

    }

#endif
};

#endif