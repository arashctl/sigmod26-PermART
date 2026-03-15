/**
 * Setbench test harness for performing rigorous data structure microbenchmarks.
 *
 * Copyright (C) 2018 Trevor Brown
 */

#define MICROBENCH

// typedef long long test_type;
typedef unsigned long test_type;  // to match the parlay generator

#include <artifact_parlay/internal/get_time.h>
#include <artifact_parlay/internal/group_by.h>
#include <artifact_parlay/io.h>
#include <artifact_parlay/primitives.h>
#include <artifact_parlay/random.h>
#include <omp.h>
#include <perftools.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <limits>
#include <parallel/algorithm>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "zipfian.h"
#if defined(USE_MIMALLOC_ON_PMEM)
#include <mimalloc.h>
#include <mimalloc/types.h>

#elif defined(USE_LIBVMMALLOC)
#include "libvmmalloc.h"
#endif

#ifndef SHIFT_FOR_PARLAY_MAXKEY
#define SHIFT_FOR_PARLAY_MAXKEY 0
#endif

#ifdef PRINT_JEMALLOC_STATS
#include <jemalloc/jemalloc.h>
#define DEBUG_PRINT_ARENA_STATS malloc_stats_print(printCallback, NULL, "ag")
void printCallback(void *nothing, const char *data) { std::cout << data; }
#else
#define DEBUG_PRINT_ARENA_STATS
#endif

/******************************************************************************
 * Configure global statistics tracking & output using GSTATS (common/gstats)
 * Note: it is crucial that this import occurs before any user headers
 * (that might use GSTATS) are included.
 *
 * This is because it define the macro GSTATS_HANDLE_STATS, which is
 * (read: should be) used by all user includes to determine whether to perform
 * any GSTATS_ calls.
 *
 * Thus, including this before all other user headers ENABLES GSTATS in them.
 *****************************************************************************/
#include "define_global_statistics.h"
#include "gstats_global.h"  // include the GSTATS code and macros (crucial this happens after GSTATS_HANDLE_STATS is defined)
// Note: any statistics trackers created by headers included below have to be
// handled separately... we do this below.

// each thread saves its own thread-id (should be used primarily within this
// file--could be eliminated to improve software engineering)
__thread int tid = 0;

enum PrefillType {
    PREFILL_INSERT,
    PREFILL_MIXED,
    PREFILL_HYBRID,
    PREFILL_PARLAY_STYLE,
    PREFILL_PARLAY_STYLE_SINGLE_PHASE
};
const char *PrefillTypeStrings[] = {"PREFILL_INSERT", "PREFILL_MIXED",
                                    "PREFILL_HYBRID", "PREFILL_PARLAY_STYLE",
                                    "PREFILL_PARLAY_STYLE_SINGLE_PHASE"};

// some read-only globals (could be encapsulated in a struct and passed around
// to improve software engineering)

#include "plaf.h"
PAD;
double INS_FRAC;
double DEL_FRAC;
double RQ;
size_t RQSIZE;
size_t MAXKEY = 0;
size_t MIN_GENERATED_KEY = 0;
size_t MAX_GENERATED_KEY = 0;
int MILLIS_TO_RUN;
int DESIRED_PREFILL_SIZE;
bool PREFILL;
int PREFILL_THREADS;
int WORK_THREADS;
int RQ_THREADS;
int ITERATOR_THREADS;
int TOTAL_THREADS;
double ZIPF_PARAM;
bool IS_SPARSE = false;  // For setbench key generators, not parlay
bool IS_PREGENERATED = false;
PrefillType PREFILL_TYPE;
int PREFILL_HYBRID_MIN_MS;
int PREFILL_HYBRID_MAX_MS;
#ifdef ParlayDistributions
int PREFILL_PARLAY_MIN_MS;
bool PARLAY_DENSE = false;
#endif
PAD;

#include "globals_extern.h"

#if defined(USE_XOSHIRO256P)
#pragma message "Using xoshiro256+ Generator"
#include "random_xoshiro256p.h"
#elif defined(USE_FNV1A)
#pragma message "Using FNV1a Generator"
#include "random_fnv1a.h"
#else
#pragma message "Using xoshiro256++ Generator (default)"
#include "random_xoshiro256pp.h"
#endif

#include "adapter.h" /* data structure adapter header (selected according to the "ds/..." subdirectory in the -I include paths */
#include "binding.h"
#include "keygen.h"
#include "papi_util_impl.h"
#include "plaf.h"
#include "rq_provider.h"
#include "tree_stats.h"
#define DS_ADAPTER_T \
    ds_adapter<test_type, VALUE_TYPE, RECLAIM<>, ALLOC<>, POOL<>>

#ifndef INSERT_FUNC
#define INSERT_FUNC insertIfAbsent
#endif

#ifdef RQ_SNAPCOLLECTOR
#define RQ_SNAPCOLLECTOR_OBJECT_TYPES                                        \
    , SnapCollector<node_t<test_type, test_type>, test_type>,                \
        SnapCollector<node_t<test_type, test_type>, test_type>::NodeWrapper, \
        ReportItem, CompactReportItem
#define RQ_SNAPCOLLECTOR_OBJ_SIZES                                      \
    << " SnapCollector="                                                \
    << (sizeof(SnapCollector<node_t<test_type, test_type>, test_type>)) \
    << " NodeWrapper="                                                  \
    << (sizeof(SnapCollector<node_t<test_type, test_type>,              \
                             test_type>::NodeWrapper))                  \
    << " ReportItem=" << (sizeof(ReportItem))                           \
    << " CompactReportItem=" << (sizeof(CompactReportItem))
#else
#define RQ_SNAPCOLLECTOR_OBJECT_TYPES
#define RQ_SNAPCOLLECTOR_OBJ_SIZES
#endif

// #ifndef IntAsValue
// #define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
// #define VALUE_TYPE void *
// #else
// #define KEY_TO_VALUE(key) 123
// #define VALUE_TYPE int
// #endif

#ifndef VALUE_TYPE
#define KEY_TO_VALUE(key) &key /* note: hack to turn a key into a pointer */
#else
#define KEY_TO_VALUE(key) key
#endif

#ifndef VALUE_TYPE
#define VALUE_TYPE void *
#endif

#ifdef USE_RCU
#include "eer_prcu_impl.h"
#define __RCU_INIT_THREAD urcu::registerThread(tid);
#define __RCU_DEINIT_THREAD urcu::unregisterThread();
#define __RCU_INIT_ALL urcu::init(TOTAL_THREADS);
#define __RCU_DEINIT_ALL urcu::deinit(TOTAL_THREADS);
#else
#define __RCU_INIT_THREAD
#define __RCU_DEINIT_THREAD
#define __RCU_INIT_ALL
#define __RCU_DEINIT_ALL
#endif

#ifdef USE_RLU
#include "rlu.h"
PAD;
__thread rlu_thread_data_t *rlu_self;
PAD;
rlu_thread_data_t *rlu_tdata = NULL;
#define __RLU_INIT_THREAD       \
    rlu_self = &rlu_tdata[tid]; \
    RLU_THREAD_INIT(rlu_self);
#define __RLU_DEINIT_THREAD RLU_THREAD_FINISH(rlu_self);
#define __RLU_INIT_ALL                                   \
    rlu_tdata = new rlu_thread_data_t[MAX_THREADS_POW2]; \
    RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
#define __RLU_DEINIT_ALL \
    RLU_FINISH();        \
    delete[] rlu_tdata;
#else
#define __RLU_INIT_THREAD
#define __RLU_DEINIT_THREAD
#define __RLU_INIT_ALL
#define __RLU_DEINIT_ALL
#endif

#define INIT_ALL    \
    __RCU_INIT_ALL; \
    __RLU_INIT_ALL;
#define DEINIT_ALL    \
    __RLU_DEINIT_ALL; \
    __RCU_DEINIT_ALL;

#define THREAD_MEASURED_PRE                                                  \
    tid = __tid;                                                             \
    binding_bindThread(tid);                                                 \
    test_type garbage = 0;                                                   \
    test_type *rqResultKeys = new test_type[RQSIZE + MAX_KEYS_PER_NODE];     \
    test_type *iterResultKeys = new test_type[1]; \
    VALUE_TYPE *rqResultValues = new VALUE_TYPE[RQSIZE + MAX_KEYS_PER_NODE]; \
    VALUE_TYPE *iterResultValues = new VALUE_TYPE[1]; \
    __RLU_INIT_THREAD;                                                       \
    __RCU_INIT_THREAD;                                                       \
    g->dsAdapter->initThread(tid);                                           \
    papi_create_eventset(tid);                                               \
    __sync_fetch_and_add(&g->running, 1);                                    \
    __sync_synchronize();                                                    \
    while (!g->start) {                                                      \
        SOFTWARE_BARRIER;                                                    \
        TRACE COUTATOMICTID("waiting to start" << std::endl);                \
    }                                                                        \
    GSTATS_SET(tid, time_thread_start,                                       \
               std::chrono::duration_cast<std::chrono::microseconds>(        \
                   std::chrono::high_resolution_clock::now() - g->startTime) \
                   .count());                                                \
    papi_start_counters(tid);                                                \
    int cnt = 0;                                                             \
    int rq_cnt = 0;                                                          \
    DURATION_START(tid);

#define THREAD_MEASURED_POST                                                 \
    __sync_fetch_and_add(&g->running, -1);                                   \
    DURATION_END(tid, duration_all_ops);                                     \
    GSTATS_SET(tid, time_thread_terminate,                                   \
               std::chrono::duration_cast<std::chrono::microseconds>(        \
                   std::chrono::high_resolution_clock::now() - g->startTime) \
                   .count());                                                \
    SOFTWARE_BARRIER;                                                        \
    papi_stop_counters(tid);                                                 \
    SOFTWARE_BARRIER;                                                        \
    while (g->running) { SOFTWARE_BARRIER; }                                 \
    g->dsAdapter->deinitThread(tid);                                         \
    __RCU_DEINIT_THREAD;                                                     \
    __RLU_DEINIT_THREAD;                                                     \
    delete[] rqResultKeys;                                                   \
    delete[] rqResultValues;                                                 \
    g->garbage += garbage;

#define THREAD_PREFILL_PRE                                             \
    tid = __tid;                                                       \
    binding_bindThread(tid);                                           \
    test_type garbage = 0;                                             \
    double insProbability =                                            \
        (INS_FRAC > 0 ? 100 * INS_FRAC / (INS_FRAC + DEL_FRAC) : 50.); \
    __RLU_INIT_THREAD;                                                 \
    __RCU_INIT_THREAD;                                                 \
    g->dsAdapter->initThread(tid);                                     \
    __sync_fetch_and_add(&g->running, 1);                              \
    while (!g->start) {                                                \
        SOFTWARE_BARRIER;                                              \
        TRACE COUTATOMICTID("waiting to start" << std::endl);          \
    }  // wait to start

#define THREAD_PREFILL_POST                  \
    __sync_fetch_and_add(&g->running, -1);   \
    SOFTWARE_BARRIER;                        \
    while (g->running) { SOFTWARE_BARRIER; } \
    g->dsAdapter->deinitThread(tid);         \
    __RCU_DEINIT_THREAD;                     \
    __RLU_DEINIT_THREAD;                     \
    g->garbage += garbage;

/******************************************************************************
 * Define global variables to store the numerical IDs of all GSTATS global
 * statistics trackers that have been defined over all files #included.
 *
 * It is CRUCIAL that this occurs AFTER ALL user #includes (so we catch ALL
 * GSTATS statistics trackers/counters/timers defined by those #includes).
 *
 * This includes the statistics trackers defined in define_global_statistics.h
 * as well any that were setup by a particular data structure / allocator /
 * reclaimer / pool / library that was #included above.
 *
 * This is a manually constructed list that you are free to add to if you
 * create, e.g., your own data structure specific statistics trackers.
 * They will only be included / printed when your data structure is active.
 *
 * If you add something here, you must also add to a few similar code blocks
 * below. Search this file for "GSTATS_" and you'll see where...
 *****************************************************************************/
GSTATS_DECLARE_ALL_STAT_IDS;
#ifdef GSTATS_HANDLE_STATS_BROWN_EXT_IST_LF
GSTATS_HANDLE_STATS_BROWN_EXT_IST_LF(__DECLARE_STAT_ID);
#endif
#ifdef GSTATS_HANDLE_STATS_POOL_NUMA
GSTATS_HANDLE_STATS_POOL_NUMA(__DECLARE_STAT_ID);
#endif
#ifdef GSTATS_HANDLE_STATS_RECLAIMERS_WITH_EPOCHS
GSTATS_HANDLE_STATS_RECLAIMERS_WITH_EPOCHS(__DECLARE_STAT_ID);
#endif
#ifdef GSTATS_HANDLE_STATS_USER
GSTATS_HANDLE_STATS_USER(__DECLARE_STAT_ID);
#endif
// Create storage for the CONTENTS of gstats counters (for MAX_THREADS_POW2
// threads)
GSTATS_DECLARE_STATS_OBJECT(MAX_THREADS_POW2);
// Create storage for the IDs of all global counters defined in
// define_global_statistics.h

#define TIMING_START(s)                                      \
    std::cout << "timing_start " << s << "..." << std::endl; \
    GSTATS_TIMER_RESET(tid, timer_duration);
#define TIMING_STOP                                                      \
    std::cout << "timing_elapsed "                                       \
              << (GSTATS_TIMER_SPLIT(tid, timer_duration) / 1000000000.) \
              << "s" << std::endl;
#ifndef OPS_BETWEEN_TIME_CHECKS
#define OPS_BETWEEN_TIME_CHECKS 100
#endif
#ifndef RQS_BETWEEN_TIME_CHECKS
#define RQS_BETWEEN_TIME_CHECKS 10
#endif
#ifdef MEASURE_LATENCY
#ifndef OPS_BETWEEN_LATENCY_MEASUREMENT
#define OPS_BETWEEN_LATENCY_MEASUREMENT 1000
#endif
#endif

#ifndef ParlayDistributions
enum KeyGeneratorDistribution { UNIFORM, ZIPF, ZIPFFAST, ZIPFYCSB };
#else
enum KeyGeneratorDistribution { UNIFORM, PARLAY_UNIFORM, PARLAY_ZIPF };
#endif

#ifdef MEASURE_LATENCY
typedef struct {
    std::vector<std::pair<int, long long>> vec;
    PAD;
} latency_measurement_t;
#endif

template <class KeyGenT, class PrefillKeyGenT, bool is_pregenerated>
struct globals_t {
#ifdef MEASURE_LATENCY
    PAD;
    latency_measurement_t latency_measurements[MAX_THREADS_POW2];
#endif
    PAD;
    // const
    VALUE_TYPE const NO_VALUE;
    const test_type
        KEY_MIN;  // must be smaller than any key that can be inserted/deleted
    const test_type
        KEY_MAX;  // must be less than std::max(), because the snap collector
                  // needs a reserved key larger than this! (and larger than any
                  // key that can be inserted/deleted)
    const long long PREFILL_INTERVAL_MILLIS;
    PAD;
    // write once
    long elapsedMillis;
    long long prefillKeySum;
    long long prefillSize;
    std::chrono::time_point<std::chrono::high_resolution_clock>
        programExecutionStartTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> endTime;
    PAD;
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    long long startClockTicks;
    PAD;
    long elapsedMillisNapping;
    std::chrono::time_point<std::chrono::high_resolution_clock>
        prefillStartTime;
    PAD;
    volatile test_type garbage;  // used to prevent optimizing out some code
    PAD;
    DS_ADAPTER_T *dsAdapter;  // the data structure
    PAD;
    std::conditional_t<is_pregenerated,
                       PregeneratedKeyGenerator<test_type, KeyGenT>, KeyGenT>
        *keygens[MAX_THREADS_POW2];
    PAD;
// We want to prefill with uniform because  Zipf generation is slow for large
// key ranges (and either way the probability of a given key being in the data
// structure is 50%).
#ifndef ParlayDistributions
    PrefillKeyGenT *prefillKeygens[MAX_THREADS_POW2];
#else
    PrefillingParlayGenerator<test_type> *prefillKeygens[MAX_THREADS_POW2];
#endif
    PAD;
    Random64
        rngs[MAX_THREADS_POW2];  // create per-thread random number generators
                                 // (padded to avoid false sharing)
    //    PAD; // not needed because of padding at the end of rngs
    volatile bool start;
    PAD;
    volatile bool done;
    PAD;
    volatile int running;  // number of threads that are running
    PAD;
    volatile bool debug_print;
    PAD;
#ifdef ParlayDistributions
#ifndef BoycottParlay
    parlay::sequence<test_type> uniqueKeys;
#else
    std::vector<test_type> uniqueKeys;
#endif
    PAD;
#endif

    globals_t(KeyGeneratorDistribution distribution)
        : NO_VALUE(
#ifndef IntAsValue
              NULL
#else
              0
#endif
              ),
          KEY_MIN(0) /*std::numeric_limits<test_type>::min()+1)*/
          ,
          KEY_MAX(std::numeric_limits<test_type>::max() - 1),
          PREFILL_INTERVAL_MILLIS(200) {
        debug_print = 0;
        srand(time(0));
        for (int i = 0; i < MAX_THREADS_POW2; ++i) { rngs[i].setSeed(rand()); }

#ifdef MEASURE_LATENCY
        for (int i = 0; i < MAX_THREADS_POW2; i++) {
            latency_measurements[i].vec.reserve(1000000);
        }
#endif

#ifndef ParlayDistributions

        // set up unique keys for sparse variations
        test_type *uniqueKeys = nullptr;
        test_type *sortedUniqueKeys = nullptr;
        test_type minKey, maxKey; 
        if (IS_SPARSE) {
            std::tie(uniqueKeys, sortedUniqueKeys, minKey, maxKey) = generateUniqueKeys<test_type>(MAXKEY, &rngs[0]);

            MIN_GENERATED_KEY = minKey;
            MAX_GENERATED_KEY = maxKey;
        } else {
            MIN_GENERATED_KEY = 1;
            MAX_GENERATED_KEY = MAXKEY;
        }

        // set up distribution data for zipfian distributions
        void *distData = nullptr;
        if (distribution == ZIPF) {
            // distData = new KeyGeneratorZipfData(MAXKEY, ZIPF_PARAM);
        } else if (distribution == ZIPFFAST) {
            // distData = new ZipfRejectionInversionSamplerData(MAXKEY);
        } else if (distribution == ZIPFYCSB) {
            distData = new YCSBZipfianGneratorData(MAXKEY, ZIPF_PARAM);
        }

#pragma omp parallel for
        for (int i = 0; i < MAX_THREADS_POW2; ++i) {
            // obviously use the same set of unique keys for prefill and
            // experiment key generators (if sparse)
            prefillKeygens[i] = new PrefillKeyGenT(&rngs[i], MAXKEY, ZIPF_PARAM,
                                                   uniqueKeys, sortedUniqueKeys, distData);
            if constexpr (is_pregenerated) {
                keygens[i] = new PregeneratedKeyGenerator<test_type, KeyGenT>(
                    &rngs[i], MAXKEY, ZIPF_PARAM, uniqueKeys, sortedUniqueKeys, distData,
                    std::max(TOTAL_THREADS, PREFILL_THREADS));
            } else {
                keygens[i] = new KeyGenT(&rngs[i], MAXKEY, ZIPF_PARAM,
                                         uniqueKeys, sortedUniqueKeys, distData);
            }
        }

#else

        int realMaxThreads = std::max(TOTAL_THREADS, PREFILL_THREADS);
        int numUniqueKeys = MAXKEY;
        int whatWouldHaveBeen = MAXKEY / 2;
        int samplesPerThread =
            (10 * whatWouldHaveBeen + 1000 * realMaxThreads) / realMaxThreads;

#ifndef SAMPLES_PER_THREAD_COEFFICIENT
#define SAMPLES_PER_THREAD_COEFFICIENT 1
#endif

        samplesPerThread *= SAMPLES_PER_THREAD_COEFFICIENT;

        COUTATOMIC(
            "Top of globals_t constructor: Number of parlay thread ids already "
            "assigned: "
            << parlay::num_thread_ids() << "\n");

#ifndef BoycottParlay
        if (!PARLAY_DENSE) {
            test_type _maxKey = (~0ul) >> 1;
            // test_type _maxKey = (~0ul);
            COUTATOMIC(
                "Number of parlay thread ids already assigned before beginning "
                "to generate unique keys: "
                << parlay::num_thread_ids() << "\n");

            COUTATOMIC("Need to generate "
                       << numUniqueKeys
                       << " unique keys. Starting by generating 20% more stuff."
                       << std::endl);
            auto extrasGenerated =
                parlay::delayed_tabulate(1.2 * numUniqueKeys, [&](size_t i) {
                    return (test_type)parlay::hash64(i) & (_maxKey);
                });
            COUTATOMIC("Generated 1.2U keys, removing duplicates\n");
            uniqueKeys = parlay::remove_duplicates(extrasGenerated);
            COUTATOMIC(
                "Number of parlay thread ids already assigned After removing "
                "duplicates: "
                << parlay::num_thread_ids() << "\n");

            COUTATOMIC("Removed duplicates, shuffling\n");
            uniqueKeys = parlay::random_shuffle(uniqueKeys);
            COUTATOMIC(
                "Number of parlay thread ids already assigned After random "
                "shuffling: "
                << parlay::num_thread_ids() << "\n");

            COUTATOMIC(
                "Shuffled, making sure they're correct by eliminating zero\n")

            uniqueKeys = parlay::tabulate(numUniqueKeys, [&](size_t i) {
                return 1 + std::min(_maxKey - 1, uniqueKeys[i]);
            });

            COUTATOMIC(
                "Number of parlay thread ids already assigned after finishing "
                "work with uniqueKeys: "
                << parlay::num_thread_ids() << "\n");

        } else {
            COUTATOMIC("Going through the dense path\n");
            uniqueKeys = parlay::random_shuffle(parlay::tabulate(
                numUniqueKeys, [&](size_t i) { return (test_type)i + 1; }));
        }

        COUTATOMIC("Generated and processed the unique keys, count: "
                   << uniqueKeys.size() << "\n");

#else  // BoycottParlay
        if (!PARLAY_DENSE) {
            test_type _maxKey = (~0ul) >> SHIFT_FOR_PARLAY_MAXKEY;
            std::vector<test_type> preliminary;
            preliminary.resize(numUniqueKeys * 1.2);
            COUTATOMIC(
                "Before generating preliminary: Number of parlay thread ids "
                "already assigned: "
                << parlay::num_thread_ids() << "\n");
#pragma omp parallel for
            for (int i = 0; i < preliminary.size(); i++) {
                preliminary[i] =
                    (test_type)parlay::hash64(i) &
                    _maxKey;  // 64-bit keys (0x000...00 to 0xFFF...FF)
            }

            COUTATOMIC("Generated 1.2U keys, removing duplicates\n");
            COUTATOMIC("Size of preliminary before: " << preliminary.size()
                                                      << "\n");

            std::sort(preliminary.begin(), preliminary.end());
            preliminary.erase(
                std::unique(preliminary.begin(), preliminary.end()),
                preliminary.end());

            // shuffle the keys
            auto rng = std::default_random_engine{};
            std::shuffle(std::begin(preliminary), std::end(preliminary), rng);

            // get the first numUniqueKeys elements
            // uniqueKeys = std::vector<test_type>(preliminary.begin(),
            // preliminary.begin() + numUniqueKeys); preliminary.clear();

            COUTATOMIC("Plus-oning to make sure they're correct\n");
            uniqueKeys.resize(numUniqueKeys);
#pragma omp parallel for
            for (int i = 0; i < numUniqueKeys; i++) {
                uniqueKeys[i] = 1 + std::min(_maxKey - 1, preliminary[i]);
            }
            preliminary.clear();

            COUTATOMIC(
                "After plus-oning: Number of parlay thread ids already "
                "assigned: "
                << parlay::num_thread_ids() << "\n");

            COUTATOMIC("Size of uniqueKeys: " << uniqueKeys.size() << "\n");
            COUTATOMIC("Size of preliminary: " << preliminary.size() << "\n");

        } else {
            COUTATOMIC(
                "Before generating the dense set: Number of parlay thread ids "
                "already assigned: "
                << parlay::num_thread_ids() << "\n");
            uniqueKeys.resize(numUniqueKeys);
#pragma omp parallel for
            for (int i = 0; i < numUniqueKeys; i++) {
                uniqueKeys[i] = (test_type)i + 1;
            }

            auto rng = std::default_random_engine{};
            std::shuffle(std::begin(uniqueKeys), std::end(uniqueKeys), rng);
            COUTATOMIC(
                "After shuffling the dense set: Number of parlay thread ids "
                "already assigned: "
                << parlay::num_thread_ids() << "\n");
        }
#endif

        // COUTATOMIC("Creting parlay prefilling keygens to use in
        // non-parlay-style prefilling styles\n");

        // for (int i = 0; i < MAX_THREADS_POW2; ++i) {
        //     prefillKeygens[i] = new
        //     PrefillingParlayGenerator<test_type>(&rngs[i], uniqueKeys);
        // }

        switch (distribution) {
            case PARLAY_UNIFORM: {

#ifndef BoycottParlay
                COUTATOMIC(
                    "Generating uniform samples for each thread, count per "
                    "thread:"
                    << samplesPerThread * realMaxThreads << "\n");
                auto allSamples = parlay::tabulate(
                    samplesPerThread * realMaxThreads, [&](size_t i) {
                        return uniqueKeys[parlay::hash64(i) % numUniqueKeys];
                    });

                COUTATOMIC("Doing omp parallel for to assign keygens\n");
                // #pragma omp parallel for
                for (int i = 0; i < MAX_THREADS_POW2; i++) {
                    auto start = (i % realMaxThreads) * samplesPerThread;
                    auto end = start + samplesPerThread;
                    // COUTATOMIC("Assigning keygen for thread " << i << " with
                    // start " << start << " and end " << end
                    //   << "\n");
                    keygens[i] = (KeyGenT *)(new ParlayGenerator<test_type>(
                        allSamples, start, end));
                }
#else

#pragma omp parallel for
                for (int i = 0; i < MAX_THREADS_POW2; i++) {
                    auto start = (i)*samplesPerThread;
                    auto end = start + samplesPerThread;
                    // TODO: maybe bind the thread?
                    keygens[i] = (KeyGenT *)(new ParlayGenerator<test_type>(
                        uniqueKeys, start, end,
                        samplesPerThread * realMaxThreads, INS_FRAC, DEL_FRAC,
                        nullptr));
                }
#endif
            } break;
            case PARLAY_ZIPF: {
#ifndef BoycottParlay

                COUTATOMIC(
                    "Generating zipfian samples for each thread, count per "
                    "thread:"
                    << samplesPerThread * realMaxThreads << "\n");
                Zipfian z(numUniqueKeys, ZIPF_PARAM);
                auto allSamples = parlay::tabulate(
                    samplesPerThread * realMaxThreads,
                    [&](size_t i) { return uniqueKeys[z(i)]; });
                allSamples = parlay::random_shuffle(allSamples);

                // #pragma omp parallel for
                for (int i = 0; i < MAX_THREADS_POW2; i++) {
                    auto start = (i % realMaxThreads) * samplesPerThread;
                    auto end = start + samplesPerThread;
                    // COUTATOMIC("Assigning keygen for thread " << i << " with
                    // start " << start << " and end " << end
                    //   << "\n");
                    keygens[i] = (KeyGenT *)(new ParlayGenerator<test_type>(
                        allSamples, start, end));
                }

#ifndef DontShuffle
                uniqueKeys = parlay::random_shuffle(uniqueKeys);
#endif

#else

                COUTATOMIC(
                    "Before creating the zipfian obj: Number of parlay thread "
                    "ids already assigned: "
                    << parlay::num_thread_ids() << "\n");
                Zipfian z(numUniqueKeys, ZIPF_PARAM);
                COUTATOMIC(
                    "After creating the zipfian obj: Number of parlay thread "
                    "ids already assigned: "
                    << parlay::num_thread_ids() << "\n");

#pragma omp parallel for
                for (int i = 0; i < MAX_THREADS_POW2; i++) {
                    auto start = (i)*samplesPerThread;
                    auto end = start + samplesPerThread;
                    // TODO: maybe bind the thread?
                    keygens[i] = (KeyGenT *)(new ParlayGenerator<test_type>(
                        uniqueKeys, start, end,
                        samplesPerThread * realMaxThreads, INS_FRAC, DEL_FRAC,
                        &z));
                }

// Is this the reason why I was not getting 50% inserts and deletes?
#ifndef DontShuffle
                auto rng = std::default_random_engine{};
                std::shuffle(std::begin(uniqueKeys), std::end(uniqueKeys), rng);
#endif

                COUTATOMIC(
                    "After creating the zipfian generators: Number of parlay "
                    "thread ids already assigned: "
                    << parlay::num_thread_ids() << "\n");
#endif
            } break;
            default: {
                setbench_error("invalid case");
            } break;
        }

        COUTATOMIC("Number of parlay thread ids already assigned: "
                   << parlay::num_thread_ids() << "\n");

#endif

        start = false;
        done = false;
        running = 0;
        dsAdapter = NULL;
        garbage = 0;
        prefillKeySum = 0;
        prefillSize = 0;
    }
    void enable_debug_print() { debug_print = 1; }
    void disable_debug_print() { debug_print = 0; }
    ~globals_t() {
        for (int i = 0; i < MAX_THREADS_POW2; ++i) {
#ifndef ParlayDistributions
            delete prefillKeygens[i];
#endif
            if (keygens[i]) delete keygens[i];
        }
    }
};

template <class GlobalsT>
void thread_timed(GlobalsT *g, int __tid) {
    THREAD_MEASURED_PRE;
    int tid = __tid;

#ifdef VERLIB_DEBUG
// std::cout << "thread_timed: tid=" << tid << std::endl;
// test_type key5 = 5;
// test_type key6 = 6;
// test_type key7 = 7;
// std::cout << "5 exists? " << g->dsAdapter->contains(tid, key5) << std::endl;
// std::cout << "6 exists? " << g->dsAdapter->contains(tid, key6) << std::endl;
// std::cout << "7 exists? " << g->dsAdapter->contains(tid, key7) << std::endl;
// std::cout << "Inserting 5" << std::endl;
// g->dsAdapter->INSERT_FUNC(tid, key5, KEY_TO_VALUE(key5));
// std::cout << "Inserting 6" << std::endl;
// g->dsAdapter->INSERT_FUNC(tid, key6, KEY_TO_VALUE(key6));
// std::cout << "5 exists? " << g->dsAdapter->contains(tid, key5) << std::endl;
// std::cout << "6 exists? " << g->dsAdapter->contains(tid, key6) << std::endl;
// std::cout << "7 exists? " << g->dsAdapter->contains(tid, key7) << std::endl;
#endif

#ifdef MEASURE_LATENCY

    // define measurement variables
    struct timespec op_start, op_end;
    long long op_duration;
    bool measured = false;
    int latencyOp;
#endif
    while (!g->done) {
        ++cnt;
        VERBOSE if (cnt && ((cnt % 1000000) == 0))
            COUTATOMICTID("op# " << cnt << std::endl);
        test_type key = g->keygens[tid]->next();

#ifdef MEASURE_LATENCY
        measured = cnt % OPS_BETWEEN_LATENCY_MEASUREMENT == 0;
#endif

        // COUTATOMIC(key << std::endl);
        // COUTATOMIC("[dbg] [" << tid << "] key: " << key << "\n");

#ifndef PregenerateOpTypes
        double op = g->rngs[tid].next(100000000) / 1000000.;
#else
        OpType op = g->keygens[tid]->nextOpType();

#endif

        if (
#ifndef PregenerateOpTypes
            op < INS_FRAC
#else
            op == OpType::InsertOp
#endif
        ) {
            TRACE COUTATOMICTID("### calling INSERT " << key << std::endl);
#ifdef MEASURE_LATENCY
            if (measured) {
                latencyOp = 0;
                clock_gettime(CLOCK_MONOTONIC, &op_start);
            }
#endif
            if (g->dsAdapter->INSERT_FUNC(tid, key, KEY_TO_VALUE(key)) ==
                g->dsAdapter->getNoValue()) {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }
#endif
                TRACE COUTATOMICTID("### completed INSERT modification for "
                                    << key << std::endl);
                GSTATS_ADD(tid, key_checksum, key);
                GSTATS_ADD(tid, num_successful_inserts, 1);
                // GSTATS_ADD(tid, size_checksum, 1);
            } else {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }
#endif
                TRACE COUTATOMICTID("### completed READ-ONLY" << std::endl);
            }
            GSTATS_ADD(tid, num_inserts, 1);
        } else if (
#ifndef PregenerateOpTypes
            op < INS_FRAC + DEL_FRAC
#else
            op == OpType::DeleteOp
#endif
        ) {
            TRACE COUTATOMICTID("### calling ERASE " << key << std::endl);
#ifdef MEASURE_LATENCY
            if (measured) {
                latencyOp = 1;
                clock_gettime(CLOCK_MONOTONIC, &op_start);
            }
#endif
            if (g->dsAdapter->erase(tid, key) != g->dsAdapter->getNoValue()) {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }

#endif
                TRACE COUTATOMICTID("### completed ERASE modification for "
                                    << key << std::endl);
                GSTATS_ADD(tid, key_checksum, -key);
                GSTATS_ADD(tid, num_successful_deletes, 1);
                // GSTATS_ADD(tid, size_checksum, -1);
            } else {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }

#endif
                TRACE COUTATOMICTID("### completed READ-ONLY" << std::endl);
            }
            GSTATS_ADD(tid, num_deletes, 1);
        } else if (
#ifndef PregenerateOpTypes
            op < INS_FRAC + DEL_FRAC + RQ
#else
            op == OpType::RangeOp
#endif
        ) {
            // TODO: make this respect KeyGenerators for non-uniform
            // distributions
            // uint64_t _key =
                // g->rngs[tid].next() % std::max((size_t)1, MAXKEY - RQSIZE) + 1;
            auto _keypair = g->keygens[tid]->nextRange(RQSIZE);
            auto _key = _keypair.first;
            auto _endKey = _keypair.second;
            assert(_key >= 1);
            assert(_key <= MAXKEY);
            // assert(_key <= std::max(1, MAXKEY - RQSIZE));
            assert(MAXKEY > RQSIZE || _key == 0);
            key = (test_type)_key;

            ++rq_cnt;
            size_t rqcnt;
#ifdef MEASURE_LATENCY
            if (measured) {
                latencyOp = 2;
                clock_gettime(CLOCK_MONOTONIC, &op_start);
            }
#endif
            if ((rqcnt = g->dsAdapter->rangeQuery(
                     tid, key, (test_type)_endKey, rqResultKeys,
                     (VALUE_TYPE *)rqResultValues))) {
                garbage +=
                    rqResultKeys[0] +
                    rqResultKeys[rqcnt - 1];  // prevent rqResultValues and
                                              // count from being optimized out
            }
#ifdef MEASURE_LATENCY
            if (measured) {
                clock_gettime(CLOCK_MONOTONIC, &op_end);
                op_duration = (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                              (op_end.tv_nsec - op_start.tv_nsec);
            }

#endif
            GSTATS_ADD(tid, num_rq, 1);
        } else {
#ifdef MEASURE_LATENCY
            if (measured) {
                latencyOp = 3;
                clock_gettime(CLOCK_MONOTONIC, &op_start);
            }
#endif
            if (g->dsAdapter->contains(tid, key)) {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }
#endif
                garbage += key;  // prevent optimizing out
            } else {
#ifdef MEASURE_LATENCY
                if (measured) {
                    clock_gettime(CLOCK_MONOTONIC, &op_end);
                    op_duration =
                        (op_end.tv_sec - op_start.tv_sec) * 1000000000 +
                        (op_end.tv_nsec - op_start.tv_nsec);
                }
#endif
            }

            GSTATS_ADD(tid, num_searches, 1);
        }
        // GSTATS_ADD(tid, num_operations, 1);

#ifdef MEASURE_LATENCY
        if (measured) {
            g->latency_measurements[tid].vec.push_back(
                std::make_pair(latencyOp, op_duration));
        }
#endif
    }
    THREAD_MEASURED_POST;

#ifdef MEASURE_LATENCY
    // print out the latency measurements

    auto &my_measurements = g->latency_measurements[tid].vec;
    if (my_measurements.size() > 0) {
        for (auto &m : my_measurements) {
            auto op = m.first;
            auto dur = m.second;
            COUTATOMIC("lat-" << tid << ": " << op << " " << dur << "\n");
        }
    }

#endif
}

template <class GlobalsT>
void thread_rq(GlobalsT *g, int __tid) {
    THREAD_MEASURED_PRE;
    while (!g->done) {
        // TODO: make this respect KeyGenerators for non-uniform distributions
        // uint64_t _key =
            // g->rngs[tid].next() % std::max((size_t)1, MAXKEY - RQSIZE) + 1;
        auto _keypair = g->keygens[tid]->nextRange(RQSIZE);
        auto _key = _keypair.first;
        auto _endKey = _keypair.second;
        assert(_key >= 1);
        // assert(_key <= MAXKEY);
        // assert(_key <= std::max(1, MAXKEY - RQSIZE));
        // assert(MAXKEY > RQSIZE || _key == 0);
        test_type key = (test_type)_key;
        size_t rqcnt;
        TIMELINE_START(tid);
        if ((rqcnt = g->dsAdapter->rangeQuery(tid, key, (test_type)_endKey,
                                              rqResultKeys,
                                              (VALUE_TYPE *)rqResultValues))) {
            garbage +=
                rqResultKeys[0];
                // rqResultKeys[rqcnt - 1];  // prevent rqResultValues and count
                                          // from being optimized out
        }
        TIMELINE_END(tid, "RQThreadOperation");
        GSTATS_ADD(tid, num_rq, 1);
        GSTATS_ADD(tid, num_operations, 1);
    }
    THREAD_MEASURED_POST;
}

template <class GlobalsT>
void thread_iterator(GlobalsT *g, int __tid) {
    THREAD_MEASURED_PRE;
    while (!g->done) {
        uint64_t key = MIN_GENERATED_KEY;
        size_t rqcnt; 
        TIMELINE_START(tid);
        if ((rqcnt = g->dsAdapter->rangeQuery(tid, key, MAX_GENERATED_KEY,
                                              iterResultKeys,
                                              (VALUE_TYPE *)iterResultValues))) {
            garbage +=
                iterResultKeys[0];
                                          // from being optimized out
        }
        TIMELINE_END(tid, "IteratorThreadOperation");
        GSTATS_ADD(tid, num_iterators, 1);
        GSTATS_ADD(tid, num_operations, 1);
    }
    THREAD_MEASURED_POST;
}

template <class GlobalsT>
void thread_prefill_with_updates(GlobalsT *g, int __tid) {
    THREAD_PREFILL_PRE;
    while (!g->done) {
        test_type key = g->prefillKeygens[tid]->next();
        double op = g->rngs[tid].next(100000000) / 1000000.;
        if (op < insProbability) {
            // if (g->debug_print) printf("inserting %lld\n", key);
            if (g->dsAdapter->INSERT_FUNC(tid, key, KEY_TO_VALUE(key)) ==
                g->dsAdapter->getNoValue()) {
                GSTATS_ADD(tid, key_checksum, key);
                GSTATS_ADD(tid, prefill_size, 1);
            }
            GSTATS_ADD(tid, num_inserts, 1);
        } else {
            // if (g->debug_print) printf("deleting %lld\n", key);
            if (g->dsAdapter->erase(tid, key) != g->dsAdapter->getNoValue()) {
                GSTATS_ADD(tid, key_checksum, -key);
                GSTATS_ADD(tid, prefill_size, -1);
            }
            GSTATS_ADD(tid, num_deletes, 1);
        }
        GSTATS_ADD(tid, num_operations, 1);
    }
    THREAD_PREFILL_POST;
}

// note: this function guarantees that exactly expectedSize keys are inserted
// into the data structure by the end
template <class GlobalsT>
void prefillInsert(GlobalsT *g, int64_t expectedSize) {
    std::cout << "Info: prefilling using INSERTION ONLY." << std::endl;
    g->startTime = std::chrono::high_resolution_clock::now();
    auto prefillStartTime = g->startTime;

    const int tid = 0;
#ifdef _OPENMP
    omp_set_num_threads(PREFILL_THREADS);
    const int ompThreads = omp_get_max_threads();
#else
    const int ompThreads = 1;
#endif

    TIMING_START("inserting " << expectedSize << " keys with " << ompThreads
                              << " threads");
#pragma omp parallel
    {
#ifdef _OPENMP
        const int tid = omp_get_thread_num();
        g->dsAdapter->initThread(tid);
        binding_bindThread(tid);
#else
        const int tid = 0;
        g->dsAdapter->initThread(tid);
#endif

#pragma omp for
        for (size_t i = 0; i < expectedSize; ++i) {
            test_type key = g->prefillKeygens[tid]->next();
            // test_type key = g->rngs[tid].next(MAXKEY) + 1;
            GSTATS_ADD(tid, num_inserts, 1);
            if (g->dsAdapter->INSERT_FUNC(tid, key, KEY_TO_VALUE(key)) ==
                g->dsAdapter->getNoValue()) {
                GSTATS_ADD(tid, key_checksum, key);
                GSTATS_ADD(tid, prefill_size, 1);

                // monitor prefilling progress (completely optional!!)
                if ((tid == 0) &&
                    (GSTATS_GET(tid, prefill_size) %
                     (100000 / std::max(1, (PREFILL_THREADS / 2)))) == 0) {
                    double elapsed_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::high_resolution_clock::now() -
                            g->prefillStartTime)
                            .count();
                    // double percent_done =
                    // GSTATS_GET_STAT_METRICS(prefill_size, TOTAL)[0].sum /
                    // (double) expectedSize;
                    double percent_done = GSTATS_GET(tid, prefill_size) *
                                          ompThreads / (double)expectedSize;
                    double magic_error_multiplier =
                        (1 +
                         (1 - percent_done) *
                             1.25);  // derived experimentally using huge trees.
                                     // super rough and silly linear estimator
                                     // for what is clearly a curve...
                    double total_estimate_ms =
                        magic_error_multiplier * elapsed_ms / percent_done;
                    double remaining_ms = total_estimate_ms - elapsed_ms;
                    printf(
                        "tid=%d thread_prefill_amount=%lld "
                        "percent_done_estimate=%.1f elapsed_s=%.0f "
                        "est_remaining_s=%.0f / %0.f\n",
                        tid, GSTATS_GET(tid, prefill_size),
                        (100 * percent_done), (elapsed_ms / 1000),
                        (remaining_ms / 1000), (total_estimate_ms / 1000));
                    fflush(
                        stdout);  // for some reason the above is stubborn and
                                  // doesn't print until too late (to watch
                                  // progress) if i don't flush explicitly.
                }
            } else {
                --i;
                continue;  // retry
            }
        }
    }
    TIMING_STOP;
}

template <class GlobalsT>
void prefillMixed(GlobalsT *g, int64_t expectedSize) {
    std::cout << "Info: prefilling using UPDATES (ins & del)." << std::endl;
    std::chrono::time_point<std::chrono::high_resolution_clock>
        prefillStartTime = std::chrono::high_resolution_clock::now();

    const double PREFILL_THRESHOLD = 0.02;
    const int MAX_ATTEMPTS = 10000;

    long long totalThreadsPrefillElapsedMillis = 0;

    std::thread *threads[MAX_THREADS_POW2];

    int sz = 0;
    int attempts;
    for (attempts = 0; attempts < MAX_ATTEMPTS; ++attempts) {
        INIT_ALL;

        // start all threads
        for (int i = 0; i < PREFILL_THREADS; ++i) {
            threads[i] =
                new std::thread(thread_prefill_with_updates<GlobalsT>, g, i);
        }

        TRACE COUTATOMIC(
            "main thread: waiting for threads to START prefilling running="
            << g->running << std::endl);
        while (g->running < PREFILL_THREADS) {}
        TRACE COUTATOMIC("main thread: starting prefilling timer..."
                         << std::endl);
        g->startTime = std::chrono::high_resolution_clock::now();

        auto prefillIntervalElapsedMillis = 0;
        __sync_synchronize();
        g->start = true;

        /**
         * START INFINITE LOOP DETECTION CODE
         */
        // amount of time for main thread to wait for children threads
        timespec tsExpected;
        tsExpected.tv_sec = 0;
        tsExpected.tv_nsec =
            g->PREFILL_INTERVAL_MILLIS * ((__syscall_slong_t)1000000);
        // short nap
        timespec tsNap;
        tsNap.tv_sec = 0;
        tsNap.tv_nsec = 200000000;  // 200ms

        nanosleep(&tsExpected, NULL);
        g->done = true;
        __sync_synchronize();

        const long MAX_NAPPING_MILLIS = 5000;
        auto elapsedMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now() - g->startTime)
                .count();
        auto elapsedMillisNapping = 0;
        while (g->running > 0 && elapsedMillisNapping < MAX_NAPPING_MILLIS) {
            nanosleep(&tsNap, NULL);
            elapsedMillisNapping =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - g->startTime)
                    .count() -
                elapsedMillis;
        }
        if (g->running > 0) {
            COUTATOMIC(std::endl);
            COUTATOMIC("Validation FAILURE: "
                       << g->running
                       << " non-responsive thread(s) [during prefill]"
                       << std::endl);
            COUTATOMIC(std::endl);
            exit(-1);
        }
        /**
         * END INFINITE LOOP DETECTION CODE
         */

        TRACE COUTATOMIC(
            "main thread: waiting for threads to STOP prefilling running="
            << g->running << std::endl);
        while (g->running > 0) {}

        for (int i = 0; i < PREFILL_THREADS; ++i) {
            threads[i]->join();
            delete threads[i];
        }

        g->start = false;
        g->done = false;

        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        totalThreadsPrefillElapsedMillis += prefillIntervalElapsedMillis;
        if (sz >= (size_t)expectedSize * (1 - PREFILL_THRESHOLD)) {
            break;
        } else {
            auto currTime = std::chrono::high_resolution_clock::now();
            auto totalElapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    currTime - prefillStartTime)
                    .count();
            // auto szConfirm = g->dsAdapter->size();
            // std::cout << " finished prefilling round "<<attempts<<" with ds
            // size: " << sz << " (CONFIRMING AT
            // "<<szConfirm<<") total elapsed time
            // "<<(totalElapsed/1000.)<<"s"<<std::endl;
            std::cout << " finished prefilling round " << attempts
                      << " with ds size: " << sz << " total elapsed time "
                      << (totalElapsed / 1000.) << "s" << std::endl;
            std::cout << "pref_round_size=" << sz << std::endl;
        }

        DEINIT_ALL;
    }
    if (attempts >= MAX_ATTEMPTS) {
        std::cerr << "ERROR: could not prefill to expected size "
                  << expectedSize << ". reached size " << sz << " after "
                  << attempts << " attempts" << std::endl;
        exit(-1);
    }
}

#ifdef ParlayDistributions

template <class GlobalsT>
void prefillParlayStyle(GlobalsT *g, int64_t expectedSize) {
    std::cout << "Info: prefilling using Parlay-style prefilling." << std::endl;
    std::chrono::time_point<std::chrono::high_resolution_clock>
        prefillStartTime = std::chrono::high_resolution_clock::now();

    long long totalThreadsPrefillElapsedMillis = 0;

    parlay::parallel_for(
        0, expectedSize * 2,
        [&](size_t i) {
            auto tid = parlay::worker_id();
            auto key = g->uniqueKeys[i];
            auto ret = g->dsAdapter->INSERT_FUNC(tid, key, KEY_TO_VALUE(key));
            if (ret == g->dsAdapter->getNoValue()) {
                GSTATS_ADD(tid, key_checksum, key);
                GSTATS_ADD(tid, prefill_size, 1);
            }
            GSTATS_ADD(tid, num_inserts, 1);
        },
        10, true);

    std::cout << "Info: Insertion phase over." << std::endl;

    parlay::parallel_for(
        0, expectedSize,
        [&](size_t i) {
            auto tid = parlay::worker_id();
            auto key = g->uniqueKeys[i];
            auto ret = g->dsAdapter->erase(tid, key);
            if (ret != g->dsAdapter->getNoValue()) {
                GSTATS_ADD(tid, key_checksum, -key);
                GSTATS_ADD(tid, prefill_size, -1);
            }
            GSTATS_ADD(tid, num_deletes, 1);
        },
        10, true);

    COUTATOMIC("Prefilling done!");
}

template <class GlobalsT>
void thread_prefill_parlay_style_insertion_phase(GlobalsT *g, int __tid,
                                                 int start, int end) {
    THREAD_PREFILL_PRE;

    // COUTATOMIC("Starting insertion phase for thread " << tid << " with start
    // " << start << " and end " << end << " / " << g->uniqueKeys.size() <<
    // "\n"); while (!g->done) {
    for (int curIndex = start; curIndex < end; curIndex++) {
        test_type key = g->uniqueKeys[curIndex];
        if (g->dsAdapter->INSERT_FUNC(tid, key, KEY_TO_VALUE(key)) ==
            g->dsAdapter->getNoValue()) {
            GSTATS_ADD(tid, key_checksum, key);
            GSTATS_ADD(tid, prefill_size, 1);
        } else {
            COUTATOMIC("tid: " << tid << ", curIndex: " << curIndex
                               << ", Insertion failed for key " << key << "\n");
        }
        GSTATS_ADD(tid, num_inserts, 1);
        GSTATS_ADD(tid, num_operations, 1);
    }
    // }

    while (!g->done) {}

    THREAD_PREFILL_POST;
}

template <class GlobalsT>
void thread_prefill_parlay_style_deletion_phase(GlobalsT *g, int __tid,
                                                int start, int end) {
    THREAD_PREFILL_PRE;

    // COUTATOMIC("Starting deletion phase for thread " << tid << " with start "
    // << start << " and end " << end << " / "
    // << g->uniqueKeys.size() << "\n");
    for (int curIndex = start; curIndex < end; curIndex++) {
        test_type key = g->uniqueKeys[curIndex];
        if (g->dsAdapter->erase(tid, key) != g->dsAdapter->getNoValue()) {
            GSTATS_ADD(tid, key_checksum, -key);
            GSTATS_ADD(tid, prefill_size, -1);
        }
        GSTATS_ADD(tid, num_deletes, 1);
        GSTATS_ADD(tid, num_operations, 1);
    }

    while (!g->done) {}

    THREAD_PREFILL_POST;
}

template <class GlobalsT>
void prefillParlayStyleWithStdThreads(GlobalsT *g, int64_t expectedSize) {
    if (expectedSize <= 0) { return; }
    using namespace std::chrono;
    std::cout << "Info: prefilling using parlay-style prefilling, but with "
                 "std::thread instead of parlay threads."
              << std::endl;

    COUTATOMIC(
        "Top of prefillParlayStyleWithStdThreads: Number of parlay thread ids "
        "already assigned: "
        << parlay::num_thread_ids() << "\n");
    g->startTime = high_resolution_clock::now();
    int64_t sz = 0;
    INIT_ALL;

    int phase1InsertCount = g->uniqueKeys.size();
    int phase2DeleteCount = phase1InsertCount - expectedSize;

    std::thread *threads[MAX_THREADS_POW2];

    // Phase 1: Insert phase1InsertCount keys (basically all unique keys in the
    // default configurations)
    for (int i = 0; i < PREFILL_THREADS; i++) {
        int tid = i;
        int perThreadSize = phase1InsertCount / PREFILL_THREADS;
        int start = tid * perThreadSize;
        int end = start + perThreadSize;
        if (i == PREFILL_THREADS - 1) { end = phase1InsertCount; }

        threads[i] = new std::thread(
            thread_prefill_parlay_style_insertion_phase<GlobalsT>, g, i, start,
            end);
    }

    TRACE COUTATOMIC(
        "main thread: waiting for threads to START prefilling running="
        << g->running << std::endl);
    SOFTWARE_BARRIER;
    while (g->running < PREFILL_THREADS) { SOFTWARE_BARRIER; }

    TRACE COUTATOMIC("main thread: starting prefilling timer..." << std::endl);

    auto now = high_resolution_clock::now();
    auto elapsedMillis =
        duration_cast<milliseconds>(now - g->startTime).count();
    std::cout << "prefilling initialization took " << (elapsedMillis / 1000.)
              << "s" << std::endl;

    g->startTime = now;

    __sync_synchronize();

    g->start = true;

    /**
     * MAIN THREAD NOW DETERMINES WHEN INSERTING THREADS SHOULD STOP (setting
     * g->done)
     */

    /**
     first, we wait for the minimum prefilling time
     */

    timespec tsMinPrefillingTime = {
        0, PREFILL_PARLAY_MIN_MS * ((__syscall_slong_t)1000000)};
    nanosleep(&tsMinPrefillingTime, NULL);

    /**
     * if we didn't sleep long enough, do so in 100ms increments
     */

    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    while (elapsedMillis < PREFILL_PARLAY_MIN_MS) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    }

    // print status update
    sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    std::cout << "prefilling_size=" << sz
              << " phase1_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
              << std::endl;

    // Now repeatedly check if you have inserted phase1InsertCount keys

    while (sz < phase1InsertCount) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();

        // print status update
        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        std::cout << "prefilling_size=" << sz
                  << " phase1_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
                  << std::endl;
    }

    /**
     * stop all phase1 prefilling
     */

    SOFTWARE_BARRIER;
    g->done = true;
    __sync_synchronize();

    // stop threads
    for (int i = 0; i < PREFILL_THREADS; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    COUTATOMIC(
        "Afther phase1 done: Number of parlay thread ids already assigned: "
        << parlay::num_thread_ids() << "\n");

    g->start = false;
    g->done = false;

    // Phase 2: Delete phase2DeleteCount keys
    auto phase2StartTime = high_resolution_clock::now();

    for (int i = 0; i < PREFILL_THREADS; i++) {
        int tid = i;
        int perThreadSize = phase2DeleteCount / PREFILL_THREADS;
        int start = tid * perThreadSize;
        int end = start + perThreadSize;
        if (i == PREFILL_THREADS - 1) { end = phase2DeleteCount; }
        threads[i] = new std::thread(
            thread_prefill_parlay_style_deletion_phase<GlobalsT>, g, i, start,
            end);
    }

    TRACE COUTATOMIC("main thread: waiting for threads to START phase2 running="
                     << g->running << std::endl);
    SOFTWARE_BARRIER;
    while (g->running < PREFILL_THREADS) { SOFTWARE_BARRIER; }

    TRACE COUTATOMIC("main thread: starting phase2 timer..." << std::endl);
    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - phase2StartTime).count();
    std::cout << "phase2 prefilling took " << (elapsedMillis / 1000.) << "s"
              << std::endl;

    // g->startTime = now;

    __sync_synchronize();
    g->start = true;

    /**
     * MAIN THREAD NOW DETERMINES WHEN DELETION THREADS SHOULD STOP (setting
     * g->done)
     */

    /**
     * first, we wait for the minimum prefilling time
     */

    /**
     first, we wait for the minimum prefilling time
     */

    tsMinPrefillingTime = {
        0, PREFILL_PARLAY_MIN_MS * ((__syscall_slong_t)1000000)};
    nanosleep(&tsMinPrefillingTime, NULL);

    /**
     * if we didn't sleep long enough, do so in 100ms increments
     */

    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - phase2StartTime).count();
    while (elapsedMillis < PREFILL_PARLAY_MIN_MS) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis =
            duration_cast<milliseconds>(now - phase2StartTime).count();
    }

    // print status update
    sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    std::cout << "prefilling_size=" << sz
              << " phase2_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
              << std::endl;

    /**
     * now repeatedly check whether the data structure is prefilled enough
     */

    while (sz > expectedSize) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis =
            duration_cast<milliseconds>(now - phase2StartTime).count();

        // print status update
        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        std::cout << "prefilling_size=" << sz
                  << " phase2_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
                  << std::endl;
    }

    SOFTWARE_BARRIER;
    g->done = true;
    __sync_synchronize();

    // stop threads
    for (int i = 0; i < PREFILL_THREADS; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    COUTATOMIC(
        "AFter phase2 done: Number of parlay thread ids already assigned: "
        << parlay::num_thread_ids() << "\n");

    g->start = false;
    g->done = false;
    DEINIT_ALL;
    g->prefillStartTime = g->startTime;
}

template <class GlobalsT>
void prefillParlayStyleWithStdThreadsSinglePhase(GlobalsT *g,
                                                 int64_t expectedSize) {
    if (expectedSize <= 0) { return; }
    using namespace std::chrono;
    std::cout << "Info: prefilling using parlay-style prefilling, but with "
                 "std::thread instead of parlay threads."
              << std::endl;

    COUTATOMIC(
        "Top of prefillParlayStyleWithStdThreads: Number of parlay thread ids "
        "already assigned: "
        << parlay::num_thread_ids() << "\n");
    g->startTime = high_resolution_clock::now();
    int64_t sz = 0;
    INIT_ALL;

    int phase1InsertCount = expectedSize;

    std::thread *threads[MAX_THREADS_POW2];

    // Phase 1: Insert phase1InsertCount keys (basically all unique keys in the
    // default configurations)
    for (int i = 0; i < PREFILL_THREADS; i++) {
        int tid = i;
        int perThreadSize = phase1InsertCount / PREFILL_THREADS;
        int start = tid * perThreadSize;
        int end = start + perThreadSize;
        if (i == PREFILL_THREADS - 1) { end = phase1InsertCount; }

        threads[i] = new std::thread(
            thread_prefill_parlay_style_insertion_phase<GlobalsT>, g, i, start,
            end);
    }

    TRACE COUTATOMIC(
        "main thread: waiting for threads to START prefilling running="
        << g->running << std::endl);
    SOFTWARE_BARRIER;
    while (g->running < PREFILL_THREADS) { SOFTWARE_BARRIER; }

    TRACE COUTATOMIC("main thread: starting prefilling timer..." << std::endl);

    auto now = high_resolution_clock::now();
    auto elapsedMillis =
        duration_cast<milliseconds>(now - g->startTime).count();
    std::cout << "prefilling initialization took " << (elapsedMillis / 1000.)
              << "s" << std::endl;

    g->startTime = now;

    __sync_synchronize();

    g->start = true;

    /**
     * MAIN THREAD NOW DETERMINES WHEN INSERTING THREADS SHOULD STOP (setting
     * g->done)
     */

    /**
     first, we wait for the minimum prefilling time
     */

    timespec tsMinPrefillingTime = {
        0, PREFILL_PARLAY_MIN_MS * ((__syscall_slong_t)1000000)};
    nanosleep(&tsMinPrefillingTime, NULL);

    /**
     * if we didn't sleep long enough, do so in 100ms increments
     */

    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    while (elapsedMillis < PREFILL_PARLAY_MIN_MS) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    }

    // print status update
    sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    std::cout << "prefilling_size=" << sz
              << " phase1_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
              << std::endl;

    // Now repeatedly check if you have inserted phase1InsertCount keys

    while (sz < phase1InsertCount) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();

        // print status update
        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        std::cout << "prefilling_size=" << sz
                  << " phase1_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
                  << std::endl;
    }

    /**
     * stop all phase1 prefilling
     */

    SOFTWARE_BARRIER;
    g->done = true;
    __sync_synchronize();

    // stop threads
    for (int i = 0; i < PREFILL_THREADS; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    COUTATOMIC(
        "Afther phase1 done: Number of parlay thread ids already assigned: "
        << parlay::num_thread_ids() << "\n");

    g->start = false;
    g->done = false;
    DEINIT_ALL;
    g->prefillStartTime = g->startTime;
}

#endif

template <class GlobalsT>
void prefillHybrid(GlobalsT *g, int64_t expectedSize) {
    using namespace std::chrono;

    std::cout << "Info: prefilling using hybrid approach; mixed then "
                 "insert-only if needed."
              << std::endl;
    g->startTime = high_resolution_clock::now();

    const double PREFILL_THRESHOLD = 0.02;
    int64_t sz = 0;
    INIT_ALL;

    // start all threads
    std::thread *threads[MAX_THREADS_POW2];
    for (int i = 0; i < PREFILL_THREADS; ++i) {
        threads[i] =
            new std::thread(thread_prefill_with_updates<GlobalsT>, g, i);
    }

    TRACE COUTATOMIC(
        "main thread: waiting for threads to START prefilling running="
        << g->running << std::endl);
    SOFTWARE_BARRIER;
    while (g->running < PREFILL_THREADS) { SOFTWARE_BARRIER; }
    TRACE COUTATOMIC("main thread: starting prefilling timer..." << std::endl);

    auto now = high_resolution_clock::now();
    auto elapsedMillis =
        duration_cast<milliseconds>(now - g->startTime).count();
    std::cout << "prefilling initialization took " << (elapsedMillis / 1000.)
              << "s" << std::endl;

    // for the purposes of prefilling for a MINIMUM time, we don't want to
    // include the preceding initialization...
    g->startTime = now;

    __sync_synchronize();
    g->start = true;

    /**
     * MAIN THREAD NOW DETERMINES WHEN PREFILLING THREADS SHOULD STOP (setting
     * g->done)
     */

    /**
     * first, we wait for the minimum prefilling time
     */

    timespec tsMinPrefillingTime = {
        0, PREFILL_HYBRID_MIN_MS * ((__syscall_slong_t)1000000)};
    nanosleep(&tsMinPrefillingTime, NULL);

    /**
     * if we didn't sleep long enough, do so in 100ms increments
     */

    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    while (elapsedMillis < PREFILL_HYBRID_MIN_MS) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    }
    // print status update
    sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    std::cout << "prefilling_size=" << sz
              << " prefilling_elapsed_millis=" << (elapsedMillis / 1000.) << "s"
              << std::endl;

    /**
     * now repeatedly check whether the data structure is prefilled enough
     */

    while (elapsedMillis < PREFILL_HYBRID_MAX_MS &&
           sz < (size_t)expectedSize * (1 - PREFILL_THRESHOLD)) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();

        // print status update
        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        std::cout << "prefilling_size=" << sz
                  << " prefilling_elapsed_millis=" << (elapsedMillis / 1000.)
                  << "s" << std::endl;
    }

    /**
     * stop all mixed prefilling
     */

    // stop mixed prefilling (threads will stop after their current/next
    // operation)
    SOFTWARE_BARRIER;
    g->done = true;
    __sync_synchronize();

    // wait until all threads have stopped running (checking for infinite loops)
    const long MAX_NAPPING_MILLIS = 10000;
    now = high_resolution_clock::now();
    elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
    int64_t elapsedMillisNapping = 0;
    while (g->running > 0 && elapsedMillisNapping < MAX_NAPPING_MILLIS) {
        timespec tsNap = {0, 100000000};
        nanosleep(&tsNap, NULL);
        elapsedMillisNapping =
            (int64_t)(duration_cast<milliseconds>(high_resolution_clock::now() -
                                                  g->startTime)
                          .count() -
                      elapsedMillis);
    }
    if (g->running > 0) {
        COUTATOMIC(std::endl
                   << "Validation FAILURE: " << g->running
                   << " non-responsive thread(s) [during prefill]" << std::endl
                   << std::endl);
        exit(-1);
    }

    // stop threads
    for (int i = 0; i < PREFILL_THREADS; ++i) {
        threads[i]->join();
        delete threads[i];
    }
    g->start = false;
    g->done = false;

    /**
     * if needed, finish up with insert-only prefilling
     */

    sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    if (sz < (size_t)expectedSize * (1 - PREFILL_THRESHOLD)) {
        std::cout << "Info: prefilling requires an additional insert phase to "
                     "complete."
                  << std::endl;

        // use insert-only prefilling to close the gap between sz and
        // expectedSize
        int64_t numKeysToInsert = expectedSize - sz;
        prefillInsert(g, numKeysToInsert);

        // print status update
        now = high_resolution_clock::now();
        elapsedMillis = duration_cast<milliseconds>(now - g->startTime).count();
        sz = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
        std::cout << "prefilling_size=" << sz
                  << " prefilling_elapsed_millis=" << (elapsedMillis / 1000.)
                  << "s" << std::endl;
    }

    DEINIT_ALL;
    g->prefillStartTime = g->startTime;  // supporting legacy output code...
}

template <class GlobalsT>
size_t *prefillArray(GlobalsT *g, int64_t expectedSize) {
    std::cout << "Info: prefilling using ARRAY CONSTRUCTION to expectedSize="
              << expectedSize << " w/MAXKEY=" << MAXKEY << "." << std::endl;
    if (MAXKEY < expectedSize)
        setbench_error(
            "specified key range must be large enough to accommodate the "
            "specified prefill size");

    TIMING_START("creating key array");
    size_t sz = MAXKEY + 2;
    const size_t DOES_NOT_EXIST = std::numeric_limits<size_t>::max();
    size_t *present = new size_t[sz];
#ifdef _OPENMP
    omp_set_num_threads(PREFILL_THREADS);
#endif
#pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < sz; ++i) present[i] = DOES_NOT_EXIST;
    TIMING_STOP;

    TIMING_START("choosing random keys with present array");
#pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < expectedSize; ++i) {
    retry:
#ifdef _OPENMP
        const int tid = omp_get_thread_num();
#else
        const int tid = 0;
#endif

        test_type key = g->prefillKeygens[tid]->next();
        // auto key = g->rngs[tid].next(MAXKEY) + 1;
        if (__sync_bool_compare_and_swap(&present[key], DOES_NOT_EXIST, key)) {
            GSTATS_ADD(tid, key_checksum, key);
            // GSTATS_ADD(tid, size_checksum, 1);
        } else {
            goto retry;
        }
    }
    TIMING_STOP;

    TIMING_START("parallel sort to obtain keys to insert");
#ifdef _OPENMP
    __gnu_parallel::sort(present, present + sz);
#else
    std::sort(present, present + sz);
#endif
    TIMING_STOP;

    return present;
}

template <class GlobalsT>
void createAndPrefillDataStructure(GlobalsT *g, int64_t expectedSize) {
    if (PREFILL_THREADS == 0) {
        g->dsAdapter =
            new DS_ADAPTER_T(std::max(PREFILL_THREADS, TOTAL_THREADS),
                             g->KEY_MIN, g->KEY_MAX, g->NO_VALUE, g->rngs);
        return;
    }

    if (expectedSize == -1) {
        const double expectedFullness =
            (INS_FRAC + DEL_FRAC ? INS_FRAC / (double)(INS_FRAC + DEL_FRAC)
                                 : 0.5);  // percent full in expectation
        expectedSize = (int64_t)(MAXKEY * expectedFullness);
    }

    // prefill data structure to mimic its structure in the steady state
    g->prefillStartTime = std::chrono::high_resolution_clock::now();

#ifdef PREFILL_BUILD_FROM_ARRAY
    // PREBUILD VIA PARALLEL ARRAY CONSTRUCTION
    auto present = prefillArray(g, expectedSize);
    TIMING_START("constructing data structure");
    g->dsAdapter = new DS_ADAPTER_T(
        std::max(PREFILL_THREADS, TOTAL_THREADS), g->KEY_MIN, g->KEY_MAX,
        g->NO_VALUE, g->rngs, (test_type const *)present,
        (VALUE_TYPE const *)present, expectedSize, rand());
    TIMING_STOP;
    delete[] present;
#else
    g->dsAdapter =
        new DS_ADAPTER_T(std::max(PREFILL_THREADS, TOTAL_THREADS), g->KEY_MIN,
                         g->KEY_MAX, g->NO_VALUE, g->rngs);

    // PREBUILD VIA REPEATED CONCURRENT INSERT-ONLY TRIALS
    if (PREFILL_TYPE == PREFILL_INSERT) {
        prefillInsert(g, expectedSize);

        // PREBUILD VIA REPEATED CONCURRENT INSERT-AND-DELETE TRIALS
    } else if (PREFILL_TYPE == PREFILL_MIXED) {
        prefillMixed(g, expectedSize);

        // PREBUILD VIA A HYBRID APPROACH (FIRST MIXED UPDATES, THEN INSERT-ONLY
        // IF NEEDED ONCE TIME IS UP)
    } else if (PREFILL_TYPE == PREFILL_HYBRID) {
        prefillHybrid(g, expectedSize);

    }
#ifdef ParlayDistributions
    else if (PREFILL_TYPE == PREFILL_PARLAY_STYLE) {
        prefillParlayStyleWithStdThreads(g, expectedSize);
    } else if (PREFILL_TYPE == PREFILL_PARLAY_STYLE_SINGLE_PHASE) {
        prefillParlayStyleWithStdThreadsSinglePhase(g, expectedSize);
    }
#endif

    else {
        setbench_error("invalid prefilling type specified");
    }
#endif

    // print prefilling status information
    using namespace std::chrono;
    const long totalUpdates =
        GSTATS_OBJECT_NAME.get_sum<long long>(num_inserts) +
        GSTATS_OBJECT_NAME.get_sum<long long>(num_deletes);
    g->prefillKeySum = GSTATS_OBJECT_NAME.get_sum<long long>(key_checksum);
    g->prefillSize = GSTATS_OBJECT_NAME.get_sum<long long>(prefill_size);
    auto now = high_resolution_clock::now();
    auto elapsedMillis =
        duration_cast<milliseconds>(now - g->startTime).count();
    COUTATOMIC("finished prefilling to size "
               << g->prefillSize << " for expected size " << expectedSize
               << " keysum=" << g->prefillKeySum << ", performing "
               << totalUpdates << " updates; total_prefilling_elapsed_ms="
               << elapsedMillis << " ms)" << std::endl);
    std::cout << "pref_size=" << g->prefillSize << std::endl;
    std::cout << "pref_millis=" << elapsedMillis << std::endl;
    GSTATS_CLEAR_ALL;

    // print total prefilling time
    std::cout << "prefill_elapsed_ms="
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() -
                     g->prefillStartTime)
                     .count()
              << std::endl;
    // g->dsAdapter->printSummary();  ///////// debug
}

template <class GlobalsT>
void trial(GlobalsT *g) {
    using namespace std::chrono;
    papi_init_program(TOTAL_THREADS);

    // create the actual data structure and prefill it to match the expected
    // steady state
    createAndPrefillDataStructure(g, DESIRED_PREFILL_SIZE);

    // setup measured part of the experiment
    INIT_ALL;

#ifdef CALL_DEBUG_GC
    g->dsAdapter->debugGCSingleThreaded();
#endif

    // TODO: reclaim all garbage floating in the record manager that was
    // generated during prefilling, so it doesn't get freed at the start of the
    // measured part of the execution? (maybe it's better not to do this, since
    // it's realistic that there is some floating garbage in the steady state.
    // that said, it's probably not realistic that it's all eligible for
    // reclamation, first thing...)

    // precompute amount of time for main thread to wait for children threads
    timespec tsExpected;
    tsExpected.tv_sec = MILLIS_TO_RUN / 1000;
    tsExpected.tv_nsec = (MILLIS_TO_RUN % 1000) * ((__syscall_slong_t)1000000);
    // precompute short nap time
    timespec tsNap;
    tsNap.tv_sec = 0;
    tsNap.tv_nsec = 100000000;  // 100ms

    // start all threads
    std::thread *threads[MAX_THREADS_POW2];
    for (int i = 0; i < TOTAL_THREADS; ++i) {
        if (i < WORK_THREADS) {
            threads[i] = new std::thread(thread_timed<GlobalsT>, g, i);
        } else if (i < WORK_THREADS + RQ_THREADS) {
            threads[i] = new std::thread(thread_rq<GlobalsT>, g, i);
        } else { 
            threads[i] = new std::thread(thread_iterator<GlobalsT>, g, i);
        }
    }

    while (g->running < TOTAL_THREADS) {
        TRACE COUTATOMIC("main thread: waiting for threads to START running="
                         << g->running << std::endl);
    }  // wait for all threads to be ready
    COUTATOMIC("main thread: starting timer..." << std::endl);

    DEBUG_PRINT_ARENA_STATS;
    COUTATOMIC(std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(
        "################################ BEGIN RUNNING "
        "################################"
        << std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(std::endl);

    SOFTWARE_BARRIER;
    g->startTime = std::chrono::high_resolution_clock::now();
    g->startClockTicks = get_server_clock();
    SOFTWARE_BARRIER;
    printUptimeStampForPERF("START");
#ifdef MEASURE_TIMELINE_STATS
    ___timeline_use = 1;
#endif
    g->start = true;
    SOFTWARE_BARRIER;

    // join is replaced with sleeping, and kill threads if they run too long
    // method: sleep for the desired time + a small epsilon,
    //      then check "g->running" to see if we're done.
    //      if not, loop and sleep in small increments for up to 5s,
    //      and exit(-1) if running doesn't hit 0.

    if (MILLIS_TO_RUN > 0) {
        nanosleep(&tsExpected, NULL);
        SOFTWARE_BARRIER;
        g->done = true;
        __sync_synchronize();
        g->endTime = std::chrono::high_resolution_clock::now();
        __sync_synchronize();
        printUptimeStampForPERF("END");
    }

    DEBUG_PRINT_ARENA_STATS;
    COUTATOMIC(std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(
        "################################## TIME IS UP "
        "#################################"
        << std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(std::endl);

    const long MAX_NAPPING_MILLIS = (MAXKEY > 5e7 ? 120000 : 30000);
    g->elapsedMillis =
        duration_cast<milliseconds>(g->endTime - g->startTime).count();
    g->elapsedMillisNapping = 0;
    while (g->running > 0 && g->elapsedMillisNapping < MAX_NAPPING_MILLIS) {
        nanosleep(&tsNap, NULL);
        g->elapsedMillisNapping =
            duration_cast<milliseconds>(high_resolution_clock::now() -
                                        g->startTime)
                .count() -
            g->elapsedMillis;
    }

    if (g->running > 0) {
        COUTATOMIC(std::endl);
        COUTATOMIC(
            "Validation FAILURE: "
            << g->running
            << " non-terminating thread(s) [did we exhaust physical memory and "
               "experience excessive slowdown due to swap mem?]"
            << std::endl);
        COUTATOMIC(std::endl);
        COUTATOMIC("elapsedMillis=" << g->elapsedMillis
                                    << " elapsedMillisNapping="
                                    << g->elapsedMillisNapping << std::endl);

        exit(-1);

        if (g->dsAdapter->validateStructure()) {
            std::cout << "Structural validation OK" << std::endl;
        } else {
            std::cout << "Structural validation FAILURE." << std::endl;
        }

#if defined USE_GSTATS && defined OVERRIDE_PRINT_STATS_ON_ERROR
        GSTATS_PRINT;
        std::cout << std::endl;
#endif

        g->dsAdapter->printSummary();
#ifdef RQ_DEBUGGING_H
        DEBUG_VALIDATE_RQ(TOTAL_THREADS);
#endif
        exit(-1);
    }

    // join all threads
    COUTATOMIC("joining threads...");
    for (int i = 0; i < TOTAL_THREADS; ++i) {
        // COUTATOMIC("joining thread "<<i<<std::endl);
        threads[i]->join();
        delete threads[i];
    }

    COUTATOMIC(std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(
        "################################# END RUNNING "
        "#################################"
        << std::endl);
    COUTATOMIC(
        "######################################################################"
        "#########"
        << std::endl);
    COUTATOMIC(std::endl);

    COUTATOMIC(((g->elapsedMillis + g->elapsedMillisNapping) / 1000.)
               << "s" << std::endl);

    papi_deinit_program();
    DEINIT_ALL;
}

template <class GlobalsT>
void printExecutionTime(GlobalsT *g) {
    auto programExecutionElapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() -
            g->programExecutionStartTime)
            .count();
    std::cout << "total_execution_walltime="
              << (programExecutionElapsed / 1000.) << "s" << std::endl;
}

template <class GlobalsT>
void printOutput(GlobalsT *g) {
    std::cout << "PRODUCING OUTPUT" << std::endl;
#ifdef USE_TREE_STATS
    auto timeBeforeTreeStats = std::chrono::high_resolution_clock::now();
    auto treeStats = g->dsAdapter->createTreeStats(g->KEY_MIN, g->KEY_MAX);
    auto timeAfterTreeStats = std::chrono::high_resolution_clock::now();
    auto elapsedTreeStats =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            timeAfterTreeStats - timeBeforeTreeStats)
            .count();
    std::cout << std::endl;
    std::cout << "tree_stats_computeWalltime=" << (elapsedTreeStats / 1000.)
              << "s" << std::endl;
    std::cout << std::endl;
    // std::cout<<"size_nodes="<<
    if (treeStats) std::cout << treeStats->toString() << std::endl;
#endif
    g->dsAdapter
        ->printSummary();  // can put this before GSTATS_PRINT to help some
                           // hacky debug code in reclaimer_ebr_token route some
                           // information to GSTATS_ to be printed. not a big
                           // deal, though.

#ifdef USE_GSTATS
    GSTATS_PRINT;
    std::cout << std::endl;
#endif

    long long threadsKeySum = 0;
    long long threadsSize = 0;

#ifdef USE_GSTATS
    {
        threadsKeySum = GSTATS_GET_STAT_METRICS(key_checksum, TOTAL)[0].sum +
                        g->prefillKeySum;
        // threadsSize = GSTATS_GET_STAT_METRICS(size_checksum, TOTAL)[0].sum +
        // g->prefillSize;
#ifdef USE_TREE_STATS
        long long dsKeySum =
            (treeStats) ? treeStats->getSumOfKeys() : threadsKeySum;
        long long dsSize =
            (treeStats) ? treeStats->getKeys() : -1;  // threadsSize;
#endif
        std::cout << "threads_final_keysum=" << threadsKeySum << std::endl;
        // std::cout<<"threads_final_size="<<threadsSize<<std::endl;
#ifdef USE_TREE_STATS
        std::cout << "final_keysum=" << dsKeySum << std::endl;
        std::cout << "final_size=" << dsSize << std::endl;
        if (threadsKeySum == dsKeySum) {  // && threadsSize == dsSize) {
            std::cout << "validate_result=success" << std::endl;
            std::cout << "Validation OK." << std::endl;
            if (treeStats == NULL)
                std::cout << "**** WARNING: VALIDATION WAS ACTUALLY _SKIPPED_ "
                             "AS THIS DS DOES NOT SUPPORT IT!"
                          << std::endl;
        } else {
            std::cout << "validate_result=fail" << std::endl;
            std::cout << "Validation FAILURE: threadsKeySum=" << threadsKeySum
                      << " dsKeySum=" << dsKeySum
                      << /*" threadsSize="<<threadsSize<<*/ " dsSize=" << dsSize
                      << std::endl;
            // std::cout<<"Validation comment: data structure is "<<(dsSize >
            // threadsSize ? "LARGER" : "SMALLER")<<" than it should be
            // according to the operation return values"<<std::endl;
            printExecutionTime(g);
            exit(-1);
        }
#endif
    }
#endif

#if !defined SKIP_VALIDATION
    if (g->dsAdapter->validateStructure()) {
        std::cout << "Structural validation OK." << std::endl;
    } else {
        std::cout << "Structural validation FAILURE." << std::endl;
        printExecutionTime(g);
        exit(-1);
    }
#endif

    long long totalAll = 0;

#ifdef USE_GSTATS
    {
        const long long totalSearches =
            GSTATS_GET_STAT_METRICS(num_searches, TOTAL)[0].sum;
        const long long totalRQs =
            GSTATS_GET_STAT_METRICS(num_rq, TOTAL)[0].sum;

        const long long totalIterators =
            GSTATS_GET_STAT_METRICS(num_iterators, TOTAL)[0].sum;
        const long long totalQueries = totalSearches + totalRQs + totalIterators;
        const long long totalInserts =
            GSTATS_GET_STAT_METRICS(num_inserts, TOTAL)[0].sum;
        const long long totalDeletes =
            GSTATS_GET_STAT_METRICS(num_deletes, TOTAL)[0].sum;
        const long long totalUpdates = totalInserts + totalDeletes;

        const double SECONDS_TO_RUN =
            (g->elapsedMillis / 1000.);  // (MILLIS_TO_RUN)/1000.;
        totalAll = totalUpdates + totalQueries;
        const long long throughputSearches =
            (long long)(totalSearches / SECONDS_TO_RUN);
        const long long throughputRQs = (long long)(totalRQs / SECONDS_TO_RUN);
        const long long totalIteratorsThroughput =
            (long long)(totalIterators / SECONDS_TO_RUN);

        const double throughputRQsFloat = (double)(totalRQs) / SECONDS_TO_RUN;
        const double throughputIteratorsFloat =
            (double)(totalIterators) / SECONDS_TO_RUN;

        const long long throughputQueries =
            (long long)(totalQueries / SECONDS_TO_RUN);
        const long long throughputUpdates =
            (long long)(totalUpdates / SECONDS_TO_RUN);
        const long long throughputAll = (long long)(totalAll / SECONDS_TO_RUN);

        COUTATOMIC(std::endl);
        COUTATOMIC("total_find=" << totalSearches << std::endl);
        COUTATOMIC("total_rq=" << totalRQs << std::endl);
        COUTATOMIC("total_iterators=" << totalIterators << std::endl);
        COUTATOMIC("total_inserts=" << totalInserts << std::endl);
        COUTATOMIC("total_deletes=" << totalDeletes << std::endl);
        COUTATOMIC("total_updates=" << totalUpdates << std::endl);
        COUTATOMIC("total_queries=" << totalQueries << std::endl);
        COUTATOMIC("total_ops=" << totalAll << std::endl);
        COUTATOMIC("find_throughput=" << throughputSearches << std::endl);
        COUTATOMIC("rq_throughput=" << throughputRQs << std::endl);
        COUTATOMIC("rq_throughput_float=" << throughputRQsFloat << std::endl);
        COUTATOMIC("iterators_throughput=" << totalIteratorsThroughput
                                          << std::endl);
        COUTATOMIC("iterators_throughput_float=" << throughputIteratorsFloat
                                                  << std::endl);
        COUTATOMIC("update_throughput=" << throughputUpdates << std::endl);
        COUTATOMIC("query_throughput=" << throughputQueries << std::endl);
        COUTATOMIC("total_throughput=" << throughputAll << std::endl);
        COUTATOMIC(std::endl);

        COUTATOMIC(std::endl);
        COUTATOMIC("total find                    : " << totalSearches
                                                      << std::endl);
        COUTATOMIC("total rq                      : " << totalRQs << std::endl);

        COUTATOMIC("total inserts                 : " << totalInserts
                                                      << std::endl);
        COUTATOMIC("total deletes                 : " << totalDeletes
                                                      << std::endl);
        COUTATOMIC("total updates                 : " << totalUpdates
                                                      << std::endl);
        COUTATOMIC("total queries                 : " << totalQueries
                                                      << std::endl);
        COUTATOMIC("total ops                     : " << totalAll << std::endl);
        COUTATOMIC("find throughput               : " << throughputSearches
                                                      << std::endl);
        COUTATOMIC("rq throughput                 : " << throughputRQs
                                                      << std::endl);

        COUTATOMIC("rq throughput (float)         : " << throughputRQsFloat
                                                      << std::endl);

        COUTATOMIC("update throughput             : " << throughputUpdates
                                                      << std::endl);
        COUTATOMIC("query throughput              : " << throughputQueries
                                                      << std::endl);
        COUTATOMIC("total throughput              : " << throughputAll
                                                      << std::endl);
        COUTATOMIC(std::endl);
    }
#endif

    COUTATOMIC("elapsed milliseconds          : " << g->elapsedMillis
                                                  << std::endl);
    COUTATOMIC("napping milliseconds overtime : " << g->elapsedMillisNapping
                                                  << std::endl);
    COUTATOMIC(std::endl);

    //    g->dsAdapter->printSummary();

    // free ds
#if !defined NO_CLEANUP_AFTER_WORKLOAD
    std::cout << "begin delete ds..." << std::endl;
    if (MAXKEY > 10000000) {
        std::cout << "    SKIPPING deletion of data structure to save time! "
                     "(because key range is so large)"
                  << std::endl;
    } else {
        delete g->dsAdapter;
    }
    std::cout << "end delete ds." << std::endl;
#endif

    papi_print_counters(totalAll);
#ifdef USE_TREE_STATS
    if (treeStats) delete treeStats;
#endif

#if !defined NDEBUG
    std::cout << "WARNING: NDEBUG is not defined, so experiment results may be "
                 "affected by assertions and debug code."
              << std::endl;
#endif
#if defined MEASURE_REBUILDING_TIME || defined MEASURE_TIMELINE_STATS || \
    defined RAPID_RECLAMATION
    std::cout << "WARNING: one or more of MEASURE_REBUILDING_TIME | "
                 "MEASURE_TIMELINE_STATS | RAPID_RECLAMATION are "
                 "defined, which *may* affect experiments results."
              << std::endl;
#endif
}

template <class GlobalsT>
void main_continued_with_globals(GlobalsT *g) {
    g->programExecutionStartTime = std::chrono::high_resolution_clock::now();

    // print object sizes, to help debugging/sanity checking memory layouts
    g->dsAdapter->printObjectSizes();

    // setup thread pinning/binding
    binding_configurePolicy(TOTAL_THREADS);

    // print actual thread pinning/binding layout
    std::cout << "ACTUAL_THREAD_BINDINGS=";
    for (int i = 0; i < TOTAL_THREADS; ++i) {
        std::cout << (i ? "," : "") << binding_getActualBinding(i);
    }
    std::cout << std::endl;
    if (!binding_isInjectiveMapping(TOTAL_THREADS)) {
        std::cout << "ERROR: thread binding maps more than one thread to a "
                     "single logical processor"
                  << std::endl;
        exit(-1);
    }

    /******************************************************************************
     * Perform the actual creation of all GSTATS global statistics trackers that
     * have been defined over all files #included.
     *
     * This includes the statistics trackers defined in
     *define_global_statistics.h as well any that were setup by a particular
     *data structure / allocator / reclaimer / pool / library that was #included
     *above.
     *
     * This is a manually constructed list that you are free to add to if you
     * create, e.g., your own data structure specific statistics trackers.
     * They will only be included / printed when your data structure is active.
     *****************************************************************************/
    std::cout << std::endl;
#ifdef GSTATS_HANDLE_STATS_BROWN_EXT_IST_LF
    GSTATS_HANDLE_STATS_BROWN_EXT_IST_LF(__CREATE_STAT);
#endif
#ifdef GSTATS_HANDLE_STATS_POOL_NUMA
    GSTATS_HANDLE_STATS_POOL_NUMA(__CREATE_STAT);
#endif
#ifdef GSTATS_HANDLE_STATS_RECLAIMERS_WITH_EPOCHS
    GSTATS_HANDLE_STATS_RECLAIMERS_WITH_EPOCHS(__CREATE_STAT);
#endif
#ifdef GSTATS_HANDLE_STATS_USER
    GSTATS_HANDLE_STATS_USER(__CREATE_STAT);
#endif
    GSTATS_CREATE_ALL;
    std::cout << std::endl;

    trial(g);
    printOutput(g);

    binding_deinit();
    std::cout
        << "garbage=" << g->garbage
        << std::endl;  // to prevent certain steps from being optimized out
    GSTATS_DESTROY;

    printExecutionTime(g);
    delete g;
}

int main(int argc, char **argv) {
#if defined(USE_MIMALLOC_ON_PMEM)

    mi_option_enable(mi_option_verbose);
    mi_option_disable(mi_option_reserve_os_memory);
    mi_option_disable(mi_option_reserve_huge_os_pages);
    mi_option_enable(mi_option_limit_os_alloc);
    mi_option_enable(mi_option_disallow_os_alloc);
    mi_option_set(mi_option_arena_reserve, 0);

#ifdef MIMALLOC_PMEM_NUMA
    size_t total_pmem_size = 90 * 1024 * 1024 * 1024ULL;
    int fd0 = open("/mnt/pmem0_mount/mkhalaji/mimalloc_playground/file.dat",
                   O_RDWR | O_CREAT, 0666);
    int fd1 = open("/mnt/pmem1_mount/mkhalaji/mimalloc_playground/file.dat",
                   O_RDWR | O_CREAT, 0666);
    ftruncate(fd0, total_pmem_size / 2);
    ftruncate(fd1, total_pmem_size / 2);

    void *addr0 = mmap(0, total_pmem_size / 2, PROT_READ | PROT_WRITE,
                       MAP_SYNC | MAP_SHARED_VALIDATE | MAP_32BIT, fd0, 0);
    void *addr1 = mmap(0, total_pmem_size / 2, PROT_READ | PROT_WRITE,
                       MAP_SYNC | MAP_SHARED_VALIDATE | MAP_32BIT, fd1, 0);

    auto aligned_start0 =
        ((uintptr_t)addr0 + MI_SEGMENT_ALIGN - 1) & ~(MI_SEGMENT_ALIGN - 1);
    auto aligned_start1 =
        ((uintptr_t)addr1 + MI_SEGMENT_ALIGN - 1) & ~(MI_SEGMENT_ALIGN - 1);

    auto aligned_end0 =
        ((uintptr_t)addr0 + total_pmem_size) & ~(MI_SEGMENT_ALIGN - 1);
    auto aligned_end1 =
        ((uintptr_t)addr1 + total_pmem_size) & ~(MI_SEGMENT_ALIGN - 1);

    flck::internal::region0_start = (void *)aligned_start0;
    flck::internal::region0_size =
        (size_t)aligned_end0 - (size_t)aligned_start0;
    flck::internal::region1_start = (void *)aligned_start1;
    flck::internal::region1_size =
        (size_t)aligned_end1 - (size_t)aligned_start1;

    bool ret = mi_manage_os_memory((void *)aligned_start0,
                                   aligned_end0 - aligned_start0, true, false,
                                   false, 0);
    ret = mi_manage_os_memory((void *)aligned_start1,
                              aligned_end1 - aligned_start1, true, false, false,
                              1);

    std::cout << "mi_manage_os_memory: " << ret << std::endl;

#else

    size_t total_pmem_size = 90 * 1024 * 1024 * 1024ULL;
    int fd = open("/mnt/pmem1_mount/mkhalaji/mimalloc_playground/file.dat",
                  O_RDWR | O_CREAT, 0666);
    ftruncate(fd, total_pmem_size);

    void *addr = mmap(NULL, total_pmem_size, PROT_READ | PROT_WRITE,
                      MAP_SYNC | MAP_SHARED_VALIDATE, fd, 0);

    std::cout << "MMAP success: " << (MAP_FAILED != addr) << std::endl;

    if (MAP_FAILED == addr) {
        std::cout << "errno: " << errno << std::endl;
        std::cout << "strerror: " << strerror(errno) << std::endl;
        return 1;
    }

    // if ((uintptr_t)addr % MI_SEGMENT_ALIGN != 0) {
    //     std::cout << "addr is not aligned to MI_SEGMENT_ALIGN" << std::endl;
    //     return 1;
    // }

    auto aligned_start =
        ((uintptr_t)addr + MI_SEGMENT_ALIGN - 1) & ~(MI_SEGMENT_ALIGN - 1);
    auto aligned_end =
        ((uintptr_t)addr + total_pmem_size) & ~(MI_SEGMENT_ALIGN - 1);

    flck::internal::region1_start = (void *)aligned_start;
    flck::internal::region1_size = (size_t)aligned_end - (size_t)aligned_start;
    bool ret =
        mi_manage_os_memory((void *)aligned_start, aligned_end - aligned_start,
                            true, false, false, -1);

    std::cout << "mi_manage_os_memory: " << ret << std::endl;

// void *__p = mi_malloc(1024);
// std::cout << "mi_malloc: " << p << std::endl;
// mi_free(p);
#endif

#endif

#ifdef USE_LIBVMMALLOC

    // size_t total_pmem_size = 90 * 1024 * 1024 * 1024ULL;
    // libvmmalloc_create("/mnt/pmem1_mount/mkhalaji/lvmlc_playground/file.dat",
    // total_pmem_size); libvmmalloc_init();

#endif

    printUptimeStampForPERF("MAIN_START");
    if (argc == 1) {
        std::cout << std::endl;
        std::cout << "Example usage:" << std::endl;
        std::cout << "LD_PRELOAD=/path/to/libjemalloc.so " << argv[0]
                  << " -nwork 64 -nprefill 64 -i 5 -d 5 -rq 0 -rqsize 1 -k "
                     "2000000 -nrq 0 -t 3000 -pin 0-15,32-47,16-31,48-63"
                  << std::endl;
        std::cout << std::endl;
        std::cout << "This command will benchmark the data structure "
                     "corresponding to this binary with 64 threads "
                     "repeatedly performing 5% key-inserts and 5% key-deletes "
                     "and 90% key-searches (and 0% range "
                     "queries with range query size set to a dummy value of 1 "
                     "key), on random keys from the key range "
                     "[0, 2000000), for 3000 ms. The data structure is "
                     "initially prefilled by 64 threads to contain "
                     "half of the key range. The -pin argument causes threads "
                     "to be pinned. The specified thread "
                     "pinning order is for one particular 64 thread system. "
                     "(Try running ``lscpu'' and looking at "
                     "``NUMA node[0-9]'' for a reasonable pinning order.)"
                  << std::endl;
        return 1;
    }

    std::cout << "binary=" << argv[0] << std::endl;

    // setup default args
    PREFILL_THREADS = 0;
    MILLIS_TO_RUN = 1000;
    RQ_THREADS = 0;
    ITERATOR_THREADS = 0;
    WORK_THREADS = 4;
    RQSIZE = 0;
    RQ = 0;
    INS_FRAC = 10;
    DEL_FRAC = 10;
    MAXKEY = 100000;
    PREFILL_HYBRID_MIN_MS = 1000;
    PREFILL_HYBRID_MAX_MS = 300000;  // 5 minutes
#ifdef ParlayDistributions
    PREFILL_PARLAY_MIN_MS = 1000;
#endif
    // note: DESIRED_PREFILL_SIZE is mostly useful for prefilling with in
    // non-uniform distributions, to get sparse key spaces of a particular size
    DESIRED_PREFILL_SIZE =
        -1;  // note: -1 means "use whatever would be expected in the steady
             // state" to get NO prefilling, set -nprefill 0
    KeyGeneratorDistribution distribution = KeyGeneratorDistribution::UNIFORM;

    PREFILL_TYPE = PREFILL_MIXED;

    // read command line args
    // example args: -i 25 -d 25 -k 10000 -rq 0 -rqsize 1000 -nprefill 8 -t 1000
    // -nrq 0 -nwork 8
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-i") == 0) {
            INS_FRAC = atof(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0) {
            DEL_FRAC = atof(argv[++i]);
        } else if (strcmp(argv[i], "-insdel") == 0) {
            INS_FRAC = atof(argv[++i]);
            DEL_FRAC = atof(argv[++i]);
        } else if (strcmp(argv[i], "-rq") == 0) {
            RQ = atof(argv[++i]);
        } else if (strcmp(argv[i], "-rqsize") == 0) {
            RQSIZE = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-k") == 0) {
            char *end;
            MAXKEY = strtoul(argv[++i], &end, 10);
            if (MAXKEY < 1) {
                setbench_error("key range cannot contain fewer than 1 key");
            }
        } else if (strcmp(argv[i], "-sparse") == 0) {
            IS_SPARSE = true;
        } else if (strcmp(argv[i], "-pregenerate") == 0) {
            IS_PREGENERATED = true;
        } else if (strcmp(argv[i], "-nrq") == 0) {
            RQ_THREADS = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-nwork") == 0) {
            WORK_THREADS = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-niter") == 0) {
            ITERATOR_THREADS = atoi(argv[++i]);
        } 
        else if (strcmp(argv[i], "-nprefill") ==
                   0) {  // num threads to prefill with
            PREFILL_THREADS = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-prefill-mixed") == 0) {  // prefilling type
            PREFILL_TYPE = PREFILL_MIXED;
        } else if (strcmp(argv[i], "-prefill-insert") ==
                   0) {  // prefilling type
            PREFILL_TYPE = PREFILL_INSERT;
        } else if (strcmp(argv[i], "-prefill-hybrid") ==
                   0) {  // prefilling type
            PREFILL_TYPE = PREFILL_HYBRID;
        } else if (strcmp(argv[i], "-prefill-hybrid-min-ms") == 0) {
            PREFILL_HYBRID_MIN_MS = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-prefill-hybrid-max-ms") == 0) {
            PREFILL_HYBRID_MAX_MS = atoi(argv[++i]);
        }
#ifdef ParlayDistributions
        else if (strcmp(argv[i], "-prefill-parlay-style") == 0) {
            PREFILL_TYPE = PREFILL_PARLAY_STYLE;
        } else if (strcmp(argv[i], "-prefill-parlay-style-single-phase") == 0) {
            PREFILL_TYPE = PREFILL_PARLAY_STYLE_SINGLE_PHASE;
        }
#endif
        else if (strcmp(argv[i], "-prefillsize") == 0) {
            DESIRED_PREFILL_SIZE = atol(argv[++i]);
        }
#ifndef ParlayDistributions
        else if (strcmp(argv[i], "-dist-zipf") == 0) {
            ZIPF_PARAM = atof(argv[++i]);
            distribution = KeyGeneratorDistribution::ZIPF;
        } else if (strcmp(argv[i], "-dist-zipf-fast") == 0) {
            ZIPF_PARAM = atof(argv[++i]);
            distribution = KeyGeneratorDistribution::ZIPFFAST;
        } else if (strcmp(argv[i], "-dist-zipf-ycsb") == 0) {
            ZIPF_PARAM = atof(argv[++i]);
            distribution = KeyGeneratorDistribution::ZIPFYCSB;
        } else if (strcmp(argv[i], "-dist-uniform") == 0) {
            distribution =
                KeyGeneratorDistribution::UNIFORM;  // default behaviour
        }
#else
        else if (strcmp(argv[i], "-dist-parlay-uniform") == 0) {
            distribution = KeyGeneratorDistribution::PARLAY_UNIFORM;
        } else if (strcmp(argv[i], "-dist-parlay-zipf") == 0) {
            distribution = KeyGeneratorDistribution::PARLAY_ZIPF;
            ZIPF_PARAM = atof(argv[++i]);
        } else if (strcmp(argv[i], "-parlay-dense") == 0) {
            PARLAY_DENSE = true;
        }
#endif
        else if (strcmp(argv[i], "-t") == 0) {
            MILLIS_TO_RUN = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-pin") ==
                   0) {                      // e.g., "-pin 1.2.3.8-11.4-7.0"
            binding_parseCustom(argv[++i]);  // e.g., "1.2.3.8-11.4-7.0"
            std::cout << "parsed custom binding: " << argv[i] << std::endl;
        } else {
            std::cout << "bad argument " << argv[i] << std::endl;
            exit(1);
        }
    }
    TOTAL_THREADS = WORK_THREADS + RQ_THREADS + ITERATOR_THREADS;

    // print used args
    PRINTS(DS_TYPENAME);
    PRINTS(FIND_FUNC);
    PRINTS(INSERT_FUNC);
    PRINTS(ERASE_FUNC);
    PRINTS(RQ_FUNC);
    PRINTS(RECLAIM);
    PRINTS(ALLOC);
    PRINTS(POOL);
    PRINTS(MAX_THREADS_POW2);
    PRINTS(CPU_FREQ_GHZ);
    PRINTI(MILLIS_TO_RUN);
    PRINTI(INS_FRAC);
    PRINTI(DEL_FRAC);
    PRINTI(RQ);
    PRINTI(RQSIZE);
    PRINTI(MAXKEY);
    PRINTI(IS_SPARSE);
    PRINTI(PREFILL_THREADS);
    PRINTI(DESIRED_PREFILL_SIZE);
    PRINTI(TOTAL_THREADS);
    PRINTI(WORK_THREADS);
    PRINTI(RQ_THREADS);
    PRINTI(ITERATOR_THREADS);
    PRINTI(distribution);
    printf("INS_DEL_FRAC=%.1f %.1f\n", INS_FRAC, DEL_FRAC);
    printf("PREFILL_TYPE=%s\n", PrefillTypeStrings[PREFILL_TYPE]);
    PRINTI(PREFILL_HYBRID_MIN_MS);
    PRINTI(PREFILL_HYBRID_MAX_MS);

    int maxThreads = std::max(TOTAL_THREADS, PREFILL_THREADS);

#ifndef ParlayDistributions
    switch (distribution) {
        case UNIFORM: {
            if (IS_SPARSE) {
                if (IS_PREGENERATED) {
                    main_continued_with_globals(
                        new globals_t<KeyGeneratorUniform<test_type, true>,
                                      KeyGeneratorUniform<test_type, true>,
                                      true>(distribution));
                } else {
                    main_continued_with_globals(
                        new globals_t<KeyGeneratorUniform<test_type, true>,
                                      KeyGeneratorUniform<test_type, true>,
                                      false>(distribution));
                }

            } else {
                if (IS_PREGENERATED) {
                    main_continued_with_globals(
                        new globals_t<KeyGeneratorUniform<test_type, false>,
                                      KeyGeneratorUniform<test_type, false>,
                                      true>(distribution));
                } else {
                    main_continued_with_globals(
                        new globals_t<KeyGeneratorUniform<test_type, false>,
                                      KeyGeneratorUniform<test_type, false>,
                                      false>(distribution));
                }
            }
        } break;
        // case ZIPF: {
        //     if (IS_SPARSE) {
        //         if (IS_PREGENERATED) {
        //             main_continued_with_globals(
        //                 new globals_t<KeyGeneratorZipf<test_type, true>,
        //                               KeyGeneratorUniform<test_type, true>,
        //                               true>(distribution));
        //         } else {
        //             main_continued_with_globals(
        //                 new globals_t<KeyGeneratorZipf<test_type, true>,
        //                               KeyGeneratorUniform<test_type, true>,
        //                               false>(distribution));
        //         }
        //     } else {
        //         if (IS_PREGENERATED) {
        //             main_continued_with_globals(
        //                 new globals_t<KeyGeneratorZipf<test_type, false>,
        //                               KeyGeneratorUniform<test_type, false>,
        //                               true>(distribution));
        //         } else {
        //             main_continued_with_globals(
        //                 new globals_t<KeyGeneratorZipf<test_type, false>,
        //                               KeyGeneratorUniform<test_type, false>,
        //                               false>(distribution));
        //         }
        //     }
        // } break;
        // case ZIPFFAST: {
        //     if (IS_SPARSE) {
        //         if (IS_PREGENERATED) {
        //             main_continued_with_globals(
        //                 new globals_t<
        //                     ZipfRejectionInversionSampler<test_type, true>,
        //                     KeyGeneratorUniform<test_type, true>, true>(
        //                     distribution));
        //         } else {
        //             main_continued_with_globals(
        //                 new globals_t<
        //                     ZipfRejectionInversionSampler<test_type, true>,
        //                     KeyGeneratorUniform<test_type, true>, false>(
        //                     distribution));
        //         }
        //     } else {
        //         if (IS_PREGENERATED) {
        //             main_continued_with_globals(
        //                 new globals_t<
        //                     ZipfRejectionInversionSampler<test_type, false>,
        //                     KeyGeneratorUniform<test_type, false>, true>(
        //                     distribution));
        //         } else {
        //             main_continued_with_globals(
        //                 new globals_t<
        //                     ZipfRejectionInversionSampler<test_type, false>,
        //                     KeyGeneratorUniform<test_type, false>, false>(
        //                     distribution));
        //         }
        //     }
        // } break;
        case ZIPFYCSB: {
            if (IS_SPARSE) {
                if (IS_PREGENERATED) {
                    main_continued_with_globals(
                        new globals_t<YCSBZipfianGenerator<test_type, true>,
                                      KeyGeneratorUniform<test_type, true>,
                                      true>(distribution));
                } else {
                    main_continued_with_globals(
                        new globals_t<YCSBZipfianGenerator<test_type, true>,
                                      KeyGeneratorUniform<test_type, true>,
                                      false>(distribution));
                }
            } else {
                if (IS_PREGENERATED) {
                    main_continued_with_globals(
                        new globals_t<YCSBZipfianGenerator<test_type, false>,
                                      KeyGeneratorUniform<test_type, false>,
                                      true>(distribution));
                } else {
                    main_continued_with_globals(
                        new globals_t<YCSBZipfianGenerator<test_type, false>,
                                      KeyGeneratorUniform<test_type, false>,
                                      false>(distribution));
                }
            }
        } break;
        default: {
            setbench_error("invalid case");
        } break;
    }

#else

    switch (distribution) {
        case PARLAY_UNIFORM: {
            if (PARLAY_DENSE) {
                main_continued_with_globals(
                    new globals_t<ParlayGenerator<test_type>, void *, false>(
                        distribution));
            } else {
                main_continued_with_globals(
                    new globals_t<ParlayGenerator<test_type>, void *, false>(
                        distribution));
            }

        } break;
        case PARLAY_ZIPF: {
            if (PARLAY_DENSE) {
                main_continued_with_globals(
                    new globals_t<ParlayGenerator<test_type>, void *, false>(
                        distribution));
            } else {
                main_continued_with_globals(
                    new globals_t<ParlayGenerator<test_type>, void *, false>(
                        distribution));
            }
        } break;
        default: {
            setbench_error("invalid case");
        } break;
    }
#endif
    printUptimeStampForPERF("MAIN_END");
    COUTATOMIC("Number of parlay thread ids already assigned: "
               << parlay::num_thread_ids() << "\n");

    return 0;
}
