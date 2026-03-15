#ifndef ART_VERLIB_ADAPTER_H
#define ART_VERLIB_ADAPTER_H

#include <csignal>
#include <iostream>

#ifdef USE_RALLOC
#if defined RALLOC_NUMA_AWARE
#include "ralloc_numa0.hpp"
#include "ralloc_numa1.hpp"
#else
#include "ralloc.hpp"
#endif
#endif


#include "errors.h"
// #include "record_manager.h"
#ifdef USE_TREE_STATS

#include "tree_stats.h"

#endif

#ifdef UseSetbenchTids

#ifdef ADD_MEDIUM_LEAF
#include "art_medium_leaves.h"
#elif defined(ADD_LOG_LEAF)


#ifdef DOUBLE_LOG_LEAF 
#include "art_double_log_leaves.h"
#else 
#include "art_singular_log_leaves.h"
#endif 


#else 
#include "art.h"
#endif
#else
#include "ds_impl_parlay_tids.h"
#endif

#define RECORD_MANAGER_T void *
#define DATA_STRUCTURE_T ordered_map<K, V>

// #ifdef MEASURE_UPDATE_LATENCY
#define LATENCY_COUNT_PER_THREAD 100000

// Thread-local fast PRNG (xorshift64)
struct FastRng {
    uint64_t state;
    FastRng(uint64_t seed) : state(seed ? seed : 1) {}
    
    uint64_t next() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }
    
    // Returns true with probability 1/10 (≈10%)
    bool sample() {
        return (next() % 10) == 0;
    }
};

thread_local FastRng tl_rng{0};

// #endif

template <typename K, typename V, class Reclaim = reclaimer_debra<K>,
          class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
   private:
    const V NO_VALUE;
    const V YES_VALUE = (V)123;
    DATA_STRUCTURE_T * ds;

    #ifdef MEASURE_UPDATE_LATENCY
    std::vector<uint64_t> *insert_latencies[128];
    std::vector<uint64_t> *erase_latencies[128];
    std::vector<uint64_t> *find_latencies[128];
    #endif

   public:
    ds_adapter(const int NUM_THREADS, const K &KEY_ANY, const K &KEY_MAX,
               const V &NO_VALUE, Random64 *const unused3)
        : NO_VALUE(NO_VALUE){
        std::cout << "NO_VALUE in adapter: " << NO_VALUE << "\n";

        #ifdef MEASURE_UPDATE_LATENCY
        std::fill(std::begin(insert_latencies), std::end(insert_latencies), nullptr);
        std::fill(std::begin(erase_latencies), std::end(erase_latencies), nullptr);
        std::fill(std::begin(find_latencies), std::end(find_latencies), nullptr);
        #endif

#ifdef USE_RALLOC
#if defined(RALLOC_NUMA_AWARE)

    RP_numa0_init("ralloc_art_numa0", 256 * 1024 * 1024 * 1024ULL);
    RP_numa1_init("ralloc_art_numa1", 256 * 1024 * 1024 * 1024ULL);

#else

    // provision 90 gigs of persistent memory
    size_t total_pmem_size = 90 * 1024 * 1024 * 1024ULL;
    RP_init("ralloc_art", total_pmem_size);

#endif
#endif 
            ds = new DATA_STRUCTURE_T(NUM_THREADS, KEY_ANY, KEY_MAX, NO_VALUE);


        // IMPORTANT ASSUMPTION: PREFILLING THREAD COUNT VS EXPERIMENT THREAD
        // COUNT ARE EQUAL
    }

    V getNoValue() {
        //        return nullptr;
        return ds->NO_VALUE;
    }

    void initThread(const int tid) {
        ds->initThread(tid);
        flck::setbench_tid = tid;

        // To empty insert latencies after prefilling 
        #ifdef MEASURE_UPDATE_LATENCY
        tl_rng = FastRng(tid + 1);

        if (insert_latencies[tid]) {
            delete insert_latencies[tid];
        }
        if (erase_latencies[tid]) {
            delete erase_latencies[tid];
        }
        if (find_latencies[tid]) {
            delete find_latencies[tid];
        }
        insert_latencies[tid] = new std::vector<uint64_t>();
        insert_latencies[tid]->reserve(LATENCY_COUNT_PER_THREAD);
        erase_latencies[tid] = new std::vector<uint64_t>();
        erase_latencies[tid]->reserve(LATENCY_COUNT_PER_THREAD);
        find_latencies[tid] = new std::vector<uint64_t>();
        find_latencies[tid]->reserve(LATENCY_COUNT_PER_THREAD);
        #endif
    }

    void deinitThread(const int tid) {
    }

    V insert(const int tid, const K &key, const V &val) {
        //        setbench_error("insert-replace functionality not implemented
        //        for this data structure"); return ds->insert(tid, key, val);
    }
V insertIfAbsent(const int tid, const K &key, const V &val) {
#ifdef MEASURE_UPDATE_LATENCY
    bool should_measure = insert_latencies[tid]->size() < LATENCY_COUNT_PER_THREAD 
                          && tl_rng.sample();
    
    std::chrono::high_resolution_clock::time_point start;
    if (should_measure) {
        start = std::chrono::high_resolution_clock::now();
    }
#endif

    verlib::thread_is_inserting = true;
    verlib::measuring_cow_time = tl_rng.sample();
    flck::thread_is_inserting = true;
    flck::measuring_cow_time = verlib::measuring_cow_time;
    auto ret = ds->insert(tid, key, val);
    verlib::thread_is_inserting = false;
    verlib::measuring_cow_time = false;
    flck::thread_is_inserting = false;
    flck::measuring_cow_time = false;

#ifdef MEASURE_UPDATE_LATENCY
    if (should_measure && ret == ds->NO_VALUE) {
        auto end = std::chrono::high_resolution_clock::now();
        insert_latencies[tid]->push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()
        );
    }
#endif
    return ret;
}

V erase(const int tid, const K &key) {
#ifdef MEASURE_UPDATE_LATENCY
    bool should_measure = erase_latencies[tid]->size() < LATENCY_COUNT_PER_THREAD 
                          && tl_rng.sample();
    
    std::chrono::high_resolution_clock::time_point start;
    if (should_measure) {
        start = std::chrono::high_resolution_clock::now();
    }
#endif

    auto ret = ds->remove(tid, key);

#ifdef MEASURE_UPDATE_LATENCY
    if (should_measure && ret != ds->NO_VALUE) {
        auto end = std::chrono::high_resolution_clock::now();
        erase_latencies[tid]->push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()
        );
    }
#endif

    return ret;
}

    V find(const int tid, const K &key) {
        #ifdef MEASURE_UPDATE_LATENCY
        bool should_measure = find_latencies[tid]->size() < LATENCY_COUNT_PER_THREAD 
                              && tl_rng.sample();
        std::chrono::high_resolution_clock::time_point start;
        if (should_measure) {
            start = std::chrono::high_resolution_clock::now();
        }
        #endif 
        auto ret = ds->find(tid, key);

        #ifdef MEASURE_UPDATE_LATENCY
        if (should_measure) {
            auto end = std::chrono::high_resolution_clock::now();
            find_latencies[tid]->push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()
            );
        }
        #endif

        return ret.has_value() ? *ret : NO_VALUE;
    }

    bool contains(const int tid, const K &key) {
        #ifdef MEASURE_UPDATE_LATENCY
    bool should_measure = find_latencies[tid]->size() < LATENCY_COUNT_PER_THREAD 
                          && tl_rng.sample();
    std::chrono::high_resolution_clock::time_point start;
    if (should_measure) {
        start = std::chrono::high_resolution_clock::now();
    }
    #endif

    auto ret = ds->find(tid, key).has_value();

    #ifdef MEASURE_UPDATE_LATENCY
    if (should_measure) {
        auto end = std::chrono::high_resolution_clock::now();
        find_latencies[tid]->push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()
        );
    }
    #endif

    return ret;
    }

    int rangeQuery(const int tid, const K &lo, const K &hi, K *const resultKeys,
                   V *const resultValues) {
        int count{0};
        int &countRef{count};
        ds->rangeDriver(tid, lo, hi, resultKeys, resultValues, countRef);
        resultKeys[0] = count; // To have some garbage so that the compiler does not optimize away the rq
        // My ART implementation doesn't really fill out resultKeys and resultValues
        return count;
    }

    void printSummary() { 
        
        // Print latency measurements
        #ifdef MEASURE_UPDATE_LATENCY

        // std::cout << "Find latencies (ns):\n";
        // for (int i = 0; i < 128; i++) {
        //     if (find_latencies[i]) {
        //         std::cout << "Thread " << i << ":\n";
        //         for (auto l : *find_latencies[i]) {
        //             std::cout << l << "\n";
        //         }
        //     }
        // }


        std::cout << "Insert latencies (ns):\n";
        for (int i = 0; i < 128; i++) {
            if (insert_latencies[i]) {
                std::cout << "Thread " << i << ":\n";
                for (auto l : *insert_latencies[i]) {
                    std::cout << l << "\n";
                }
            }
        }

        std::cout << "Erase latencies (ns):\n";
        for (int i = 0; i < 128; i++) {
            if (erase_latencies[i]) {
                std::cout << "Thread " << i << ":\n";
                for (auto l : *erase_latencies[i]) {
                    std::cout << l << "\n";
                }
            }
        }
        #endif

        // ds->printStats();
        return; 
    }

    bool validateStructure() { return true; }

    void printObjectSizes() { return; }

    // try to clean up: must only be called by a single thread as part of the
    // test harness!
    void debugGCSingleThreaded() { return; }
#ifdef USE_TREE_STATS

    class NodeHandler {
       public:
        using NodePtrType = typename ordered_map<K, V>::node *;
        using FullNode = typename ordered_map<K, V>::full_node;
        using SparseNode = typename ordered_map<K, V>::sparse_node;
        using LeafT = typename ordered_map<K, V>::template generic_leaf<0>;
        #ifdef ADD_LOG_LEAF
        using LogLeafT =
            typename ordered_map<K, V>::template generic_log_leaf<0>;
        #endif 
        using IndirectNode = typename ordered_map<K, V>::indirect_node;

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
                switch (node->nt) {
                    case Leaf: {
                        return false;
                    }
                    #ifdef ADD_LOG_LEAF
                    case LogLeaf: {
                        return false;
                    }
                    #endif 
                    case Full: {
                        return ix >= 0 && ix < 256;
                    }
                    case Indirect: {
                        if (ix >= 256) return false;
                        auto iN = (IndirectNode *)node;
                        for (int i = ix; i < 256; i++) {
#ifdef OrderedIndirectNodes
                            auto [which_word, which_bit] =
                                iN->word_idx_and_bit(i);
                            auto mask = 1ULL << which_bit;
                            if (iN->bitmap[which_word] & mask) {
                                ix = i;
                                return true;
                            }
#else
                            if (iN->idx[i] != -1) {
                                ix = i;
                                return true;
                            }
#endif
                        }
                        return false;
                    }
                    case Sparse: {
                        auto sN = (SparseNode *)node;
                        return ix >= 0 && ix < sN->size;
                    }
                }
                return false;
            }

            NodePtrType next() {
                switch (node->nt) {
                    case Leaf: {
                        return nullptr;
                    }
                    #ifdef ADD_LOG_LEAF
                    case LogLeaf: {
                        return nullptr;
                    }
                    #endif 
                    case Full: {
                        auto fN = (FullNode *)node;
                        return (NodePtrType)(fN->children[ix++].load());
                    }
                    case Indirect: {
                        auto iN = (IndirectNode *)node;
#ifdef OrderedIndirectNodes
                        auto [which_word, which_bit] =
                            iN->word_idx_and_bit(ix++);
                        int j = iN->ones_to_left(which_word, which_bit);
                        return (NodePtrType)(iN->ptr[j].load());
#else
                        int j = iN->idx[ix++];
                        if (j != -1) {
                            return (NodePtrType)(iN->ptr[j].load());
                        }
#endif
                        return nullptr;
                    }
                    case Sparse: {
                        auto sN = (SparseNode *)node;
                        return (NodePtrType)(sN->ptr[ix++].load());
                    }
                }
                return nullptr;
            }
        };

        static bool isLeaf(NodePtrType node) { return node->nt == Leaf || node->nt == LogLeaf; }

        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }

        static size_t getNumChildren(NodePtrType node) {
            switch (node->nt) {
                case Leaf: {
                    return 0;
                }
                case Full: {
                    return 256;
                }
                case Indirect: {
                    size_t ret = 0;
                    int j;
                    auto iN = (IndirectNode *)node;
#ifdef OrderedIndirectNodes
                    for (int j = 0; j < 4; j++) {
                        ret += __builtin_popcountll(iN->bitmap[j]);
                    }
#else
                    for (int i = 0; i < 256; i++) {
                        j = iN->idx[i];
                        if (j != -1) { ret++; }
                    }
#endif
                    return ret;
                }
                case Sparse: {
                    auto sN = (SparseNode *)node;
                    return sN->size;
                }
            }
            return 0;
        }

        static size_t getNumKeys(NodePtrType node) {

            GSTATS_ADD(0, node_type_prevalence_root, 1);

            if (node->nt == Leaf || node->nt == LogLeaf) { 

                if (node->nt == LogLeaf) { 
                    GSTATS_ADD(0, node_type_prevalence_log, 1);
                } else { 
                    auto l = (LeafT *)node;
                    if (l->size > 2) {
                        GSTATS_ADD(0, node_type_prevalence_big, 1);
                    } else {
                        GSTATS_ADD(0, node_type_prevalence_small, 1);
                    }
                }
            }


            switch (node->nt) {
                case Leaf: {
                    size_t ret = 0;
                    auto l = (LeafT *)node;
                    ret = l->size;

                    return ret;
                }
                #ifdef ADD_LOG_LEAF
                
                #ifdef DOUBLE_LOG_LEAF
                case LogLeaf: { 
                    auto ll = (LogLeafT *)node;
                    auto keys = ll->getKeys();
                    auto ret = keys.size();
                    return ret;
                }
                #else 
                case LogLeaf: {
                    size_t ret = 0; 
                    auto l = (LogLeafT *)node;

                    if (l->log.is_valid()) { 
                        if (l->log.is_delete()){
                            return l->size - 1;
                        } else { 
                            return l->size +1;
                        }
                    } else { 
                        return l->size;
                    }

                }
                #endif 
                #endif

                case Full: {
                    GSTATS_ADD(0, node_type_prevalence_full, 1);
                    return 0;
                }
                case Indirect: {
                    GSTATS_ADD(0, node_type_prevalence_indirect, 1);
                    return 0;
                }
                case Sparse: {
                    GSTATS_ADD(0, node_type_prevalence_sparse, 1);
                    return 0;
                }
            }
            return 0;
        }

        static size_t getSumOfKeys(NodePtrType node) {
            switch (node->nt) {
                case Leaf: {
                    size_t ret = 0;
                    auto l = (LeafT *)node;
                    for (int i = 0; i < l->size; i++) {
                        ret += l->key_vals[i].key;
                        //                        std::cout << "Adding " <<
                        //                        std::hex << l->key_vals[i].key
                        //                        << std::dec << " to the
                        //                        sum.\n";
                    }
                    return ret;
                }
                #ifdef ADD_LOG_LEAF

                #ifdef DOUBLE_LOG_LEAF
                case LogLeaf: { 
                    auto ll = (LogLeafT *)node;
                    auto ret = ll->getSumOfKeys(); 
                    return ret;
                }
                #else 

                case LogLeaf: {
                    size_t ret = 0; 
                    auto l = (LogLeafT *)node;
                    for (int i = 0; i < l->size; i++) {
                        ret += l->key_vals[i].key;
                    }

                    auto& log = l->log;
                    if (log.is_valid()) {
                        if (log.is_delete()) {
                            ret -= log.key;
                        } else {
                            ret += log.key;
                        }
                    }

                    return ret;
                }
                #endif 
                #endif
                case Full: {
                    return 0;
                }
                case Indirect: {
                    return 0;
                }
                case Sparse: {
                    return 0;
                }
            }
            return 0;
        }

        static size_t getSizeInBytes(NodePtrType node) { return 0; }
    };

    TreeStats<NodeHandler> *createTreeStats(const K &_minKey,
                                            const K &_maxKey) {
        std::cout << "Creating the node handler\n";
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey),
                                          ds->root, false);
    }

#endif
};

#endif
