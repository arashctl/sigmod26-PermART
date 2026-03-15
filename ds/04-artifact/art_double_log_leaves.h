#include <artifact_parlay/primitives.h>
#include <artifact_verlib/verlib.h>

#include <chrono>
#include <iostream>
#include <set>

#include "artifact_verlib/timestamps.h"
#include "durable_tools.h"
#include "gstats.h"

#if !defined(HashLock) || !defined(RemoveLockFieldFromNodes)
#define GET_LOCK(nodeptr) ((nodeptr)->lck)
#else
verlib::lock lck;
#define GET_LOCK(nodeptr) (*lck.get_lock(nodeptr))
#endif

#ifndef ALIGNMENT_SIZE
#define ALIGNMENT_SIZE 64
#endif

#ifndef MAX_INDIRECT_SIZE
#define MAX_INDIRECT_SIZE 64
#endif

#ifndef MAX_SPARSE_SIZE
#define MAX_SPARSE_SIZE 17
#endif

#ifndef MAX_SMALL_LEAF_SIZE
#define MAX_SMALL_LEAF_SIZE 2
#endif

#ifndef MAX_BIG_LEAF_SIZE
#define MAX_BIG_LEAF_SIZE 14
#endif

// #define ART_GSTATS 1
#ifdef ART_GSTATS
#define ART_GSTATS_ADD(tid, field, val) GSTATS_ADD(tid, field, val)
#else
#define ART_GSTATS_ADD(tid, field, val)
#endif


// Try rdtscp instead
long art_rdtsc() {
    unsigned int lo, hi, aux;
    __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux));
    return (long)(((uint64_t)(hi & ~(1 << 31)) << 32) | lo);
}


template <typename K>
struct int_string {
    static int length(const K &key) { return sizeof(K); }

    static int get_byte(const K &key, int pos) {
        return (key >> (8 * (sizeof(K) - 1 - pos))) & 255;
    }
};

enum node_type : char { Full, Indirect, Sparse, Leaf, LogLeaf };

template <typename K, typename V, typename String = int_string<K>>
struct ordered_map {
    V NO_VALUE;

    constexpr static int max_indirect_size = MAX_INDIRECT_SIZE;
    constexpr static int max_sparse_size = MAX_SPARSE_SIZE;
    static constexpr int max_small_leaf_size = MAX_SMALL_LEAF_SIZE;
    static constexpr int max_big_leaf_size = MAX_BIG_LEAF_SIZE;

    struct header : verlib::versioned {
        const node_type nt;
        char size;
        // every node has a byte position in the key
        // e.g. the root has byte_num = 0
        short int byte_num;
        K key;

        bool is_leaf() { return nt == Leaf || nt == LogLeaf; }

        header(node_type nt, char size) : nt(nt), size(size) {}

        header(const K &key, node_type nt, char size, int byte_num)
            : key(key), nt(nt), size(size), byte_num((short int)byte_num) {}
    };
    // generic node
    struct node : header {
        verlib::atomic_bool removed;
#if !defined(HashLock) || !defined(RemoveLockFieldFromNodes)
        verlib::lock lck;
#endif

        node(node_type nt, char size) : header(nt, size), removed(false) {}

        node(const K &key, node_type nt, char size, int byte_num)
            : header(key, nt, size, byte_num), removed(false) {}
    };

    node *root;

    using node_ptr = verlib::versioned_ptr<node>;

    // 256 entries, one for each value of a byte, null if empty

#ifdef AlignFull
    struct alignas(ALIGNMENT_SIZE) full_node : node {
#else
    struct full_node : node {
#endif
        node_ptr children[256];

        bool is_full() { return false; }

        node_ptr *get_child(const K &k) {
            auto b = String::get_byte(k, header::byte_num);
            return &children[b];
        }

        void init_child(int k, node *c) {
            auto b = String::get_byte(k, header::byte_num);
            children[b].init(c);
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(tid, this, sizeof(full_node));
        }

        full_node() : node(Full, 0) {}
    };
    // Up to max_indirect_size entries, with array of 256 1-byte
    // pointers to the entries.  Adding a new child requires a copy.
    // Updating an existing child is done in place.

#ifdef AlignIndirect
    struct alignas(ALIGNMENT_SIZE) indirect_node : node {
#else
    struct indirect_node : node {
#endif

#ifdef OrderedIndirectNodes
        // need a 256-bit bitmap
        uint64_t bitmap[4] = {0, 0, 0, 0};
        node_ptr ptr[max_indirect_size];

        bool is_full() { return node::size == max_indirect_size; }

        // debugging only
        inline int total_popcount() {
            int res = 0;
            for (int i = 0; i < 4; i++) {
                res += __builtin_popcountll(bitmap[i]);
            }
            return res;
        }

        inline std::pair<int, int> word_idx_and_bit(int byteval) {
            assert(byteval >= 0 && byteval <= 255);

            int which_word = byteval / 64;
            int which_bit = 63 - (byteval % 64);

            return {which_word, which_bit};
        }

        inline int ones_to_left(int which_word, int which_bit) {
            int res = 0;
            for (int i = 0; i < which_word; i++) {
                res += __builtin_popcountll(bitmap[i]);
            }

            if (which_bit != 63) {
                auto right_and_self_mask = (1ULL << (which_bit + 1)) - 1;
                auto left = bitmap[which_word] & (~right_and_self_mask);
                res += __builtin_popcountll(left);
            }

            return res;
        }

        node_ptr *get_child(const K &k) {
            assert(total_popcount() == node::size);
            auto [which_word, which_bit] =
                this->word_idx_and_bit(String::get_byte(k, header::byte_num));

            auto mask = 1ULL << which_bit;

            if (bitmap[which_word] & mask) {
                int idx = this->ones_to_left(which_word, which_bit);
                assert(idx < node::size && idx >= 0);
                return &ptr[idx];
            } else {
                return nullptr;
            }
        }

        void init_child(const K &k, node *c) {
            // IMPORTANT: It has to be called at the right time to keep the ptr
            // array sorted! This is never called on an ordered indirect node
            // because of implementation details
            assert(false);
            // auto [which_word, which_bit] =
            // this->word_idx_and_bit(String::get_byte(k, header::byte_num));
            // auto mask = 1ULL << which_bit;
            // bitmap[which_word] |= mask;
            // int i = this->ones_to_left(which_word, which_bit);
            // assert(i < node::size && i >= 0);
            // ptr[i].init(c);
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(tid, this, sizeof(indirect_node));
        }

        // an empty indirect node
        indirect_node() : node(Indirect, 0){};

#else
        char idx[256];
        node_ptr ptr[max_indirect_size];

        bool is_full() { return node::size == max_indirect_size; }

        node_ptr *get_child(const K &k) {
            int i = idx[String::get_byte(k, header::byte_num)];
            if (i == -1)
                return nullptr;
            else
                return &ptr[i];
        }

        void init_child(const K &k, node *c) {
            int i = node::size - 1;
            idx[String::get_byte(k, header::byte_num)] = i;
            ptr[i].init(c);
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(tid, this, sizeof(indirect_node));
        }

        // an empty indirect node
        indirect_node() : node(Indirect, 0){};
#endif
    };

// Up to max_sparse_size entries each consisting of a key and
// pointer.  The keys are immutable, but the pointers can be
// changed.  i.e. Adding a new child requires copying, but updating
// a child can be done in place.
#ifdef USE_GUYS_ALLOCATOR
    struct sparse_node : node {
#else
    struct alignas(ALIGNMENT_SIZE) sparse_node : node {
#endif
        unsigned char keys[max_sparse_size];
        node_ptr ptr[max_sparse_size];

        bool is_full() { return node::size == max_sparse_size; }

        node_ptr *get_child(const K &k) {
            __builtin_prefetch(((char *)ptr) + 64);
            int kb = String::get_byte(k, header::byte_num);
#ifndef NDEBUG
            bool found = false;
            for (int i = 0; i < node::size; i++) {
                if (keys[i] == kb && !found) {
                    found = true;
                } else if (keys[i] == kb && found) {
                    assert(false);
                }
            }
#endif

#ifdef SPARSE_NODE_SIMD

            __m128i key = _mm_set1_epi8(kb);
            __m128i keys_vec = _mm_loadu_si128((__m128i *)keys);
            __m128i cmp = _mm_cmpeq_epi8(key, keys_vec);
            int mask = _mm_movemask_epi8(cmp);

            // int trad_idx = -1;
            // for (int i = 0; i < node::size; i++) {
            //     if (keys[i] == kb) {
            //         trad_idx = i;
            //         break;
            //     }
            // }

            if (mask > 0) {
                int idx = __builtin_ctz(mask);
                // assert(idx == trad_idx);
                // COUTATOMIC("sparse node SIMD: " << idx << " " << trad_idx <<
                // " " << mask << "\n");

                if (idx < node::size) { return &ptr[idx]; }
            }

            // if (trad_idx != -1) {
            //     return &ptr[trad_idx];
            // }

#else
            for (int i = 0; i < node::size; i++)
                if (keys[i] == kb) return &ptr[i];

#endif

            return nullptr;
        }

        void init_child(const K &k, node *c) {
            int kb = String::get_byte(k, header::byte_num);
#ifndef NDEBUG
            for (int i = 0; i < node::size - 1; i++) {
                if (keys[i] == kb) { assert(false); }
            }
#endif
            keys[node::size - 1] = kb;
            ptr[node::size - 1].init(c);
        }

        // constructor for a new sparse node with two children
        sparse_node(int byte_num, node *v1, const K &k1, node *v2, const K &k2)
            : node(k1, Sparse, 2, byte_num) {
            // if (k1 == k2) {
            //         std::cout << "new sparse: " << k1 << ", " << k2 << ", "
            //         << byte_num << std::endl; abort();
            // }
            keys[0] = String::get_byte(k1, byte_num);
            ptr[0].init(v1);
            keys[1] = String::get_byte(k2, byte_num);
            ptr[1].init(v2);

            assert(keys[0] != keys[1]);
        }

        // this constructor is called during an insert to the tree
        // if a leaf node is too big, it's replaced with a sparse node, which
        // will contain the leaf split into multiple leaves
        sparse_node(int byte_num, node **start, node **end)
            : node((*start)->key, Sparse, end - start, byte_num) {
            for (int i = 0; i < (end - start); i++) {
                keys[i] = String::get_byte((*(start + i))->key, byte_num);
                ptr[i] = *(start + i);
            }

#ifndef NDEBUG
            // no duplicates
            for (int i = 0; i < node::size - 1; i++) {
                for (int j = i + 1; j < node::size; j++) {
                    if (keys[i] == keys[j]) { assert(false); }
                }
            }
#endif
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(tid, this, sizeof(sparse_node));
        }

        // an empty sparse node
        sparse_node() : node(Sparse, 0) {}
    };

    struct KV {
        K key;
        V value;
    };

    struct LogEntry : verlib::versioned_ts_only {
        K key;
        V value;
        std::atomic<uint8_t> meta;

        static constexpr uint8_t INSERT = 1 << 0;
        static constexpr uint8_t DELETE = 1 << 1;

        static constexpr uint8_t UNPERSISTED = 1 << 6;

        static constexpr uint8_t VALID = 1 << 7;

        LogEntry() : key(0), value(0), meta(0) {}

        bool is_valid() {
            bool ret = (meta & VALID);
            if (!ret) { return false; }

#if defined(DO_PERSIST_LOG_ENTRIES) && \
    defined(DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES)
            // If the valid bit is set, I still need to wait until it's
            // persisted before reading it
            if (is_unpersisted()) { wait_for_persisted(); }
#endif

            set_stamp();
            assert(is_stamped());

            return true;
        }

        inline bool is_unpersisted() const { return meta & UNPERSISTED; }
        inline bool is_persisted() const { return !(meta & UNPERSISTED); }
        inline bool is_delete() const { return meta & DELETE; }

#if defined(DO_PERSIST_LOG_ENTRIES)
        void persist(const int tid, LogEntry *otherLog) {
            // assert(is_valid());

            ART_GSTATS_ADD(tid, append_codepath_persist_root, 1);

#if defined(DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES)
            assert(is_unpersisted());
#endif

            // #ifndef NO_FLUSH_LOG_ENTRIES
            // durableTools::FLUSH_LINES(tid, this, sizeof(LogEntry));
            // #endif

#if defined(NO_FLUSH_LOG_ENTRIES)
// Experimental case, where I flush nothing. This is just for the sake of
// experimentation
#elif defined(FLUSH_THE_OTHER_LOG_ENTRY)
            // Experimental case, where I flush the other log entry. This is
            // just for the sake of experimentation
            durableTools::FLUSH_LINES(tid, otherLog, sizeof(LogEntry));
#else
            ART_GSTATS_ADD(tid, append_codepath_flush_log_entry, 1);
            // default case, where I flush the log entry that I just wrote into
            durableTools::FLUSH_LINES(tid, this, sizeof(LogEntry));
#endif

            // LOGLEAFTODO: do I need a fence here?
            // Maybe a sfence before the relaxed store?

#if defined(DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES)
            // LOGLEAFTODO: SEQ CST store here
            // Maybe do a relaxed here

            ART_GSTATS_ADD(tid, append_codepath_clear_unpersisted_bit, 1);

            meta &= ~UNPERSISTED;  // clear the unpersisted bit
            // It's compiling to either an mfence or an exchange
            // Do an atomic exchange to clear the bit instead
            // make a volatile ptr and store to it

#endif
        }
#endif

#if defined(DO_PERSIST_LOG_ENTRIES) && \
    defined(DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES)
        void wait_for_persisted() const {
            while (is_unpersisted()) {
                for (int i = 0; i < LogEntrySpinPauseCount; i++) {
                    _mm_pause();
                }
            }
            assert(!is_unpersisted());
        }
#endif
    };

    // Predeclare the generic leaf
    template <int MaxSize>
    struct generic_leaf;

    template <int MaxSize>
    struct alignas(ALIGNMENT_SIZE) generic_log_leaf : header {
        using log_leaf_ptr = generic_log_leaf<max_big_leaf_size> *;
        using leaf_ptr = generic_leaf<0> *;

#ifdef MatchNewLayout
        char padding[32];
#endif

        LogEntry log;
        LogEntry secondLog;
        KV key_vals[MaxSize];

        void print() {
            std::cout << "LogLeaf: \n";

            if (log.is_valid()) {
                std::cout << "LogEntry: " << log.key << "\n";
                std::cout << "Log is delete: " << log.is_delete() << "\n";
            }

            std::cout << "Keys: \n";

            for (int i = 0; i < header::size; i++) {
                std::cout << key_vals[i].key << " ";
            }

            std::cout << "\n------------------------------\n";
        }

        // For debugging, and treestats
        size_t getSumOfKeys() {
            size_t ret = 0;

            for (int i = 0; i < header::size; i++) { ret += key_vals[i].key; }

            if (this->log.is_valid()) {
                if (this->log.is_delete()) {
                    ret -= this->log.key;
                } else {
                    ret += this->log.key;
                }
            }

            if (this->secondLog.is_valid()) {
                if (this->secondLog.is_delete()) {
                    ret -= this->secondLog.key;
                } else {
                    ret += this->secondLog.key;
                }
            }

            return ret;
        }

        std::vector<K> getKeys() {  // for debugging, and treestats
            std::vector<K> keys;

            for (int i = 0; i < header::size; i++) {
                keys.push_back(key_vals[i].key);
            }

            if (this->log.is_valid()) {
                if (this->log.is_delete()) {
                    // Remove the key from the vector
                    auto it =
                        std::find(keys.begin(), keys.end(), this->log.key);
                    if (it != keys.end()) {
                        keys.erase(it);
                    } else {
                        assert(false);
                    }
                } else {
                    keys.push_back(this->log.key);
                }
            }

            if (this->secondLog.is_valid()) {
                if (this->secondLog.is_delete()) {
                    // Remove the key from the vector
                    auto it = std::find(keys.begin(), keys.end(),
                                        this->secondLog.key);
                    if (it != keys.end()) {
                        keys.erase(it);
                    } else {
                        assert(false);
                    }
                } else {
                    keys.push_back(this->secondLog.key);
                }
            }

            return keys;
        }

        // starts comparing a and b at the start byte (prior bytes are equal)
        // -1 if a < b, 0 if equal and 1 if a > b
        static int cmp(const K &a, const K &b, int start_byte) {
            int la = String::length(a);
            int lb = String::length(b);
            int l = std::min(la, lb);
            int i = start_byte;
            while (i < l && String::get_byte(a, i) == String::get_byte(b, i)) {
                i++;
            }
            if (i == l) return (la < lb) ? -1 : ((la == lb) ? 0 : 1);
            return (String::get_byte(a, i) < String::get_byte(b, i)) ? -1 : 1;
        }

        // returns the first byte position where the keys differ
        static int first_diff(int byte_pos, KV *start, KV *end) {
            // base case: only one key
            if (end - start == 1) return String::length(start->key);

            int j = byte_pos;
            while (true) {  // run until you find a differing byte

                // compare j'th byte of all keys in the range

                // j'th byte of the first key
                int byteval = String::get_byte(start->key, j);

                for (KV *ptr = start + 1; ptr < end; ptr++) {
                    // if you reach the end of the key, return j
                    // or
                    // if the j'th byte of the key is different from the
                    // byteval, return j
                    if (j >= String::length(ptr->key) ||
                        String::get_byte(ptr->key, j) != byteval)
                        return j;
                }
                j++;
            }
        }

        // static: it doesn't edit the instance
        // like insertion sort but with a twist
        // KV *in is already sorted
        // KV *out is the result of inserting key, value into in
        static void insert(KV *in, KV *out, int n, const K &key,
                           const V &value) {
            int i = 0;
            while (i < n && in[i].key < key) {
                out[i] = in[i];
                i++;
            }
            out[i] = KV{key, value};
            while (i < n) {
                out[i + 1] = in[i];
                i++;
            }
        }

        static void insert_and_include_log(KV *in, KV *out, int n, const K &key,
                                           const V &value, LogEntry &log) {
            assert(log.is_valid());
            assert(!log.is_delete());

            // Write kvs in in to out until you find the right place to insert
            // the new kv Also copy the key and value of the log entry into out
            // when you find the right place

#ifndef NDEBUG
            // Assert sorted order in input
            for (int i = 1; i < n; i++) {
                auto a = in[i - 1].key;
                auto b = in[i].key;

                assert(a < b);
            }
#endif

            K log_key = log.key;
            V log_value = log.value;

            assert(log_key != key);

            K k1, k2;
            V v1, v2;

            if (log_key > key) {
                k1 = key;
                v1 = value;
                k2 = log_key;
                v2 = log_value;
            } else {
                k1 = log_key;
                v1 = log_value;
                k2 = key;
                v2 = value;
            }

            int i = 0;
            int j = 0;

            while (i < n && in[i].key < k1) {
                assert(i <= n);
                assert(j <= n + 2);
                out[j] = in[i];
                i++;
                j++;
            }

            out[j] = KV{k1, v1};
            j++;

            while (i < n && in[i].key < k2) {
                assert(i <= n);
                assert(j <= n + 2);
                out[j] = in[i];
                i++;
                j++;
            }

            out[j] = KV{k2, v2};
            j++;

            while (i < n) {
                assert(i <= n);
                assert(j <= n + 2);
                out[j] = in[i];
                i++;
                j++;
            }

            assert(i == n);
            assert(j == n + 2);  // n + 2 because we inserted two new kvs

#ifndef NDEBUG

            for (int i = 1; i < n + 2; i++) {
                // Assert sorted order
                auto a = out[i - 1].key;
                auto b = out[i].key;
                assert(a < b);
            }

#endif
        }

        static void insert_and_include_both_logs(KV *in, KV *out, int n,
                                                 const K &key, const V &value,
                                                 LogEntry &log,
                                                 LogEntry &secondLog) {
            assert(log.is_valid());
            assert(secondLog.is_valid());
            assert(!log.is_delete());
            assert(!secondLog.is_delete());

            KV kv1, kv2, kv3;
            KV kvx, kvy, kvz;
            kvx = KV{log.key, log.value};
            kvy = KV{secondLog.key, secondLog.value};
            kvz = KV{key, value};

            KV kvs[3] = {kvx, kvy, kvz};
            std::sort(kvs, kvs + 3,
                      [](const KV &a, const KV &b) { return a.key < b.key; });
            kv1 = kvs[0];
            kv2 = kvs[1];
            kv3 = kvs[2];

            // Write kvs in in to out until you find the right place to insert

            int i = 0;
            int j = 0;

            while (i < n && in[i].key < kv1.key) {
                assert(i <= n);
                assert(j <= n + 3);
                out[j] = in[i];
                i++;
                j++;
            }

            out[j] = kv1;
            j++;

            while (i < n && in[i].key < kv2.key) {
                assert(i <= n);
                assert(j <= n + 3);
                out[j] = in[i];
                i++;
                j++;
            }

            out[j] = kv2;
            j++;

            while (i < n && in[i].key < kv3.key) {
                assert(i <= n);
                assert(j <= n + 3);
                out[j] = in[i];
                i++;
                j++;
            }
            out[j] = kv3;
            j++;

            while (i < n) {
                assert(i <= n);
                assert(j <= n + 3);
                out[j] = in[i];
                i++;
                j++;
            }

            assert(i == n);
            assert(j == n + 3);  // n + 3 because we inserted three new kvs
        }

        // Input: KV array in + Delete log entry + new kv pair
        // The delete log entry's key is guaranteed to be different from the new
        // kv pair being inserted Output: KV array out, excluding the log entry
        // Input size: n pairs in in, 1 log entry (to delete), 1 new kv pair
        // Output size: n - 1 + 1 = n pairs in out
        static void insert_and_exclude_log(KV *in, KV *out, int n, const K &key,
                                           const V &value, LogEntry &log) {
            K kToRemove = log.key;
            assert(key != kToRemove);

            // I have n elements in in
            // I have an element in log that I need to remove
            // I have a new element to insert
            // I should end up with n - 1 + 1 = n elements in out

#ifndef NDEBUG
            // Assert sorted order in input
            for (int i = 1; i < n; i++) {
                auto a = in[i - 1].key;
                auto b = in[i].key;

                assert(a < b);
            }
#endif

            int i = 0;
            int j = 0;

            while (i < n && in[i].key < key) {
                if (in[i].key == kToRemove) {
                    i++;
                    continue;
                }
                out[j] = in[i];
                i++;
                j++;
            }

            out[j] = KV{key, value};
            j++;

            while (i < n) {
                if (in[i].key == kToRemove) {
                    i++;
                    continue;
                }
                out[j] = in[i];
                i++;
                j++;
            }

#ifndef NDEBUG

            for (int i = 1; i < n; i++) {
                // Assert sorted order
                auto a = out[i - 1].key;
                auto b = out[i].key;

                assert(a < b);
            }

#endif
        }

        static void insert_and_exclude_both_logs(KV *in, KV *out, int n,
                                                 const K &key, const V &value,
                                                 LogEntry &log,
                                                 LogEntry &secondLog) {
            assert(log.is_valid());
            assert(secondLog.is_valid());
            assert(log.is_delete());
            assert(secondLog.is_delete());

            int i = 0;
            int j = 0;

            while (i < n && in[i].key < key) {
                if (in[i].key == log.key || in[i].key == secondLog.key) {
                    i++;
                    continue;
                }
                out[j] = in[i];
                i++;
                j++;
            }
            out[j] = KV{key, value};
            j++;
            while (i < n) {
                if (in[i].key == log.key || in[i].key == secondLog.key) {
                    i++;
                    continue;
                }
                out[j] = in[i];
                i++;
                j++;
            }
        }

        inline bool is_appendable() {
            // Only called with the lock held
            // The valid bit in the log entry is set after the log entry is
            // written and persisted If the log entry is valid, it's not
            // appendable
            // return !log.is_valid();
            return (!log.is_valid()) || (!secondLog.is_valid());
        }

        void append(const int tid, const K &key, const V &value,
                    const bool is_delete) {
            assert(is_appendable());

            #ifdef MEASURE_COW_TIME
            long start;
            if (verlib::measuring_cow_time) {
                start = art_rdtsc();
            }
            #endif

#ifndef NDEBUG

#endif

            // To reason about appending to log entries WRT timestamps, let's
            // draw parallels between logs and CoW In CoW, the new node is fully
            // initialized, except for the timestamp field So, the timestamp
            // field is initially populated with TBD Then, the versioned pointer
            // is swung to the new node, and then the writer (or a helping
            // reader) will set the timestamp field

            // In log entries, if I set K and V fields first, nobody will read
            // the entry yet because IS_VALID is false So setting IS_VALID as
            // true is analogous to swinging the versioned pointer to the new
            // node, which makes the log entry visible to everyone As a result,
            // here's the order of operations:
            // 1. Set the key and value fields of the log entry
            // 2. Set the meta field to VALID, INSERT or DELETE
            // 3. Set the timestamp field (which can be also done by readers
            // that are helping)

            // Now, the above description was for in-memory operations
            // If we add persistent to the equation, we'll have to do something
            // like the unpersisted bit So, the order of operations is:
            // 1. Set the key and value fields of the log entry
            // 2. Set the meta field: VALID | (INSERT or DELETE) | UNPERSISTED
            // 3. Persist the log entry (Flush the appropriate cache line)
            // 4. Unset the UNPERSISTED bit in the meta field
            // 5. Set the timestamp field (which can be also done by readers
            // that are helping)

            // Any reader that sees the log entry with UNPERSISTED bit set will
            // know that it has to wait for the log entry to be persisted before
            // reading it Any reader that sees the log entry with TBD timestamp
            // will need to help set it.

            ART_GSTATS_ADD(tid, append_codepath_root, 1);

            LogEntry *logToUse = &log;
            LogEntry *otherLog = &secondLog;
            if (log.is_valid()) {
                logToUse = &secondLog;
                otherLog = &log;
                ART_GSTATS_ADD(tid, append_codepath_second_log, 1);
            } else {
                ART_GSTATS_ADD(tid, append_codepath_first_log, 1);
            }

            (*logToUse).key = key;
            (*logToUse).value = value;

#if defined(DO_PERSIST_LOG_ENTRIES) && \
    defined(DO_ADD_UNPERSISTENT_BIT_TO_LOG_ENTRIES)
            uint8_t tempMeta =
                LogEntry::VALID |
                (is_delete ? LogEntry::DELETE : LogEntry::INSERT) |
                LogEntry::UNPERSISTED;
#else
            uint8_t tempMeta = LogEntry::VALID | (is_delete ? LogEntry::DELETE
                                                            : LogEntry::INSERT);
#endif

            (*logToUse).meta.store(
                tempMeta);  // Can this be just a relaxed store?

#if defined(DO_PERSIST_LOG_ENTRIES)
            // After this store, the log entry is visible to readers, but they
            // must make sure it's persisted before reading it
            (*logToUse).persist(tid, otherLog);
#else
#endif

                #ifdef MEASURE_COW_TIME
                if (verlib::measuring_cow_time) {
                    auto end = art_rdtsc();
                    auto duration = end - start;
                    GSTATS_ADD(tid, insert_time_log_append, duration);
                }
                #endif

            // Take care of timestamp
            // LOGLEAFTODO TODO

            (*logToUse).set_stamp();
            assert((*logToUse).is_stamped());

            if (log.is_valid() && secondLog.is_valid()) {
                if (log.is_delete() == secondLog.is_delete()) {
                    assert(log.key != secondLog.key);
                }
            }
        }

        bool kvsAreSorted() {
            for (int i = 1; i < header::size; i++) {
                if (key_vals[i - 1].key >= key_vals[i].key) { return false; }
            }
            return true;
        }

        bool noNullValues() {
            for (int i = 0; i < header::size; i++) {
                if (key_vals[i].value == nullptr) { return false; }
            }
            return true;
        }

        std::optional<V> find(const K &k, int byte_pos) {
            // Check the second log first
            if (secondLog.is_valid()) {
                assert(log.is_valid());
                if (secondLog.key == k) {
                    if (secondLog.is_delete()) return {};
                    return secondLog.value;
                }
            }

            // Now check the first log
            if (log.is_valid()) {
                if (log.key == k) {
                    if (log.is_delete()) return {};
                    return log.value;
                }
            }

            // Now check the key_vals
            for (int i = 0; i < header::size; i++) {
                if (!(key_vals[i].key < k)) {
                    if (k < key_vals[i].key) {
                        return {};
                    } else {
                        return key_vals[i].value;
                    }
                }
            }

            return {};
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(
                tid, this, sizeof(generic_log_leaf<max_big_leaf_size>));
        }

        // Create a multi-kv log leaf
        generic_log_leaf(int byte_pos, KV *start, KV *end)
            : header(start->key, LogLeaf, end - start,
                     first_diff(byte_pos, start, end)) {
            for (int i = 0; i < (end - start); i++) key_vals[i] = start[i];
        }

        // CoW add kv pair to leaf
        // From a regular leaf
        generic_log_leaf(int byte_pos, leaf_ptr l, K &key, V &value)
            : header(l->key, LogLeaf, l->size + 1, byte_pos) {
            // Because there's no big leaves
            assert(l->size == max_small_leaf_size);

            insert(l->key_vals, key_vals, l->size, key, value);
            assert(this->kvsAreSorted());
            assert(this->noNullValues());
        }

        // CoW add kv pair to leaf
        // From a non-appendable log leaf
        generic_log_leaf(int byte_pos, log_leaf_ptr ll, K &key, V &value)
            : header(ll->key, LogLeaf, ll->size, byte_pos) {
            // The lock on ll is held

            assert(!ll->is_appendable());
            assert(ll->log.is_valid());
            assert(ll->secondLog.is_valid());

            if (ll->log.key == ll->secondLog.key) {
                // Should be rare
                // If this happens, the log entries cancel each other out
                // So they can't be both delete or both insert
                assert(ll->log.is_delete() != ll->secondLog.is_delete());

                if (!ll->log.is_delete()) {
                    // If the key in logs is first inserted then deleted, it
                    // doesn't exist in the KV array Whether that key is equal
                    // to the key that I'm inserting or not, I should just
                    // insert the new kv pair into the kv array
                    header::size = ll->size + 1;  // n - 1 + 1 + 1 = n + 1
                    insert(ll->key_vals, key_vals, ll->size, key, value);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // // If the key in logs is first deleted then inserted, it
                    // exists in the KV array
                    // // If it's equal to the key that I'm inserting, I should
                    // just update the value in the kv array
                    // // If it's not equal to the key that I'm inserting, I
                    // should just insert the new kv pair into the kv array, and
                    // get rid of the kv pair that is deleted by the log entry
                    // if (ll->log.key == key) {
                    //     // Update the value for the kv pair that matches the
                    //     key that I'm inserting header::size = ll->size; // n
                    //     - 1 + 1 = n for (int i = 0; i < ll->size; i++) {
                    //         if (ll->key_vals[i].key == key) {
                    //             key_vals[i].value = value;
                    //             break;
                    //         }
                    //     }
                    //     assert(this->kvsAreSorted());
                    // } else {
                    //     // Insert the new kv pair into the kv array,
                    //     // and get rid of the kv pair that is deleted by the
                    //     log entry header::size = ll->size; // The element
                    //     that's deleted by the log entry is removed, and the
                    //     new element is inserted
                    //     insert_and_exclude_log(ll->key_vals, key_vals,
                    //     ll->size, key,
                    //                          value, ll->log);
                    //     assert(this->kvsAreSorted());
                    // }

                    // If the key in the logs is first deleted then inserted, it
                    // definitely doesn't equal the key that I'm inserting It
                    // also definitely exists in the kv array, so the value must
                    // be updated I should just insertion-sort the new kv pair
                    // into the kv array, and update the value for the key in
                    // the log
                    assert(ll->log.key != key);

                    V newValue = ll->secondLog.value;
                    assert(newValue != nullptr);

                    int i = 0;
                    int j = 0;

                    insert(ll->key_vals, key_vals, ll->size, key, value);

                    for (int i = 0; i < ll->size + 1; i++) {
                        if (key_vals[i].key == ll->log.key) {
                            // Update the value for the kv pair that matches the
                            // key that I'm inserting
                            key_vals[i].value = newValue;
                            break;
                        }
                    }

                    header::size = ll->size + 1;  // n - 1 + 1 + 1 = n + 1
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }

            } else if ((!ll->log.is_delete()) && (!ll->secondLog.is_delete())) {
                // Both log entries are inserts
                // They can't be the same as the key that I'm inserting, because
                // I would not have been here
                assert(ll->log.key != ll->secondLog.key);

                // insertion-sort the new kv pair and the two log entries into
                // the kv array
                insert_and_include_both_logs(ll->key_vals, key_vals, ll->size,
                                             key, value, ll->log,
                                             ll->secondLog);

                header::size = ll->size + 3;  // n + 2 + 1 = n + 3
                assert(this->kvsAreSorted());
                assert(this->noNullValues());
            } else if (ll->log.is_delete() && ll->secondLog.is_delete()) {
                // Both log entries are deletes
                // They might be the same as the key that I'm inserting
                // So I should be careful with header::size

                assert(ll->log.key != ll->secondLog.key);
                header::size = ll->size - 1;  // n - 2 + 1 = n - 1

                if (ll->log.key == key || ll->secondLog.key == key) {
                    // The key that I'm inserting has been deleted by one of the
                    // logs So it must exist in the KV array So I don't really
                    // have to do any kind of insertion sorting here because the
                    // key is already there I'm just going to ignore the log
                    // entry that deletes it, and update its value in the array
                    // Also I'm gonna ignore the other kv pair deleted by the
                    // other log entry

                    // then insertion-sort the new kv pair into the kv array,
                    // while getting rid of the other kv that's deleted by the
                    // other log entry

                    if (ll->log.key == key) {
                        // Ignore the kv corresponding to the OTHER log entry
                        // For this log entry, do not remove the kv pair, but
                        // update its value

                        int i = 0;
                        int j = 0;
                        while (i < ll->size) {
                            if (ll->key_vals[i].key == ll->secondLog.key) {
                                // Clean this up from kv array
                                i++;
                                continue;
                            }
                            key_vals[j] = ll->key_vals[i];
                            if (ll->key_vals[i].key == key) {
                                // Update the value for the kv pair that matches
                                // the key that I'm inserting
                                key_vals[j].value = value;
                            }
                            j++;
                            i++;
                        }
                        assert(this->kvsAreSorted());
                        assert(this->noNullValues());

                    } else {
                        // Ignore the kv corresponding to the OTHER log entry
                        // For this log entry, do not remove the kv pair, but
                        // update its value

                        int i = 0;
                        int j = 0;
                        while (i < ll->size) {
                            if (ll->key_vals[i].key == ll->log.key) {
                                // Clean this up from kv array
                                i++;
                                continue;
                            }
                            key_vals[j] = ll->key_vals[i];
                            if (ll->key_vals[i].key == key) {
                                // Update the value for the kv pair that matches
                                // the key that I'm inserting
                                key_vals[j].value = value;
                            }
                            j++;
                            i++;
                        }

                        assert(this->kvsAreSorted());
                        assert(this->noNullValues());
                    }
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // The key that I'm inserting has not been deleted by either
                    // log entry insertion-sort the new kv pair while ignoring
                    // the two kv pairs that are deletes
                    insert_and_exclude_both_logs(ll->key_vals, key_vals,
                                                 ll->size, key, value, ll->log,
                                                 ll->secondLog);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }

            } else {
                // One log entry is insert and the other is delete
                // but the keys are not the same
                // So I can look at my log entries calmly and be sure that
                // they're operating on different keys Because the edge case has
                // been handled above
                assert(ll->log.key != ll->secondLog.key);
                header::size = ll->size + 1;  // n - 1 + 1 + 1 = n + 1

                if (ll->log.is_delete() && ll->log.key == key) {
                    // The key that I'm inserting has been deleted by the first
                    // log entry So I should ignore the first log entry Also,
                    // the kv itself in the kv array needs to be updated with my
                    // new value because this new value is updated

                    // insert the other log entry into the kv array
                    insert(ll->key_vals, key_vals, ll->size, ll->secondLog.key,
                           ll->secondLog.value);

                    // Update the value for the old key that equals the key that
                    // I'm inserting now
                    for (int i = 0; i < ll->size + 1; i++) {
                        if (key_vals[i].key == key) {
                            key_vals[i].value = value;
                            break;
                        }
                    }
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else if (ll->secondLog.is_delete() &&
                           ll->secondLog.key == key) {
                    // The key that I'm inserting has been deleted by the second
                    // log entry So I should ignore the second log entry Also,
                    // the kv itself in the log entry needs to be updated with
                    // my new value because this new value is updated

                    // insert the other log entry into the kv array
                    insert(ll->key_vals, key_vals, ll->size, ll->log.key,
                           ll->log.value);

                    // Update the value for the old key that equals the key that
                    // I'm inserting now
                    for (int i = 0; i < ll->size + 1; i++) {
                        if (key_vals[i].key == key) {
                            key_vals[i].value = value;
                            break;
                        }
                    }
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // There is a delete log entry, but the key that I'm
                    // inserting is not the same as the log entry So I should
                    // reconcile that with one of the kv pairs that match it
                    // There is also an insert log entry, which should be
                    // written to the appropriate spot in the kv array The
                    // delete log entry is working on a different key than I am

                    assert(ll->log.key != key);
                    assert(ll->secondLog.key != key);

                    KV kvToNewlyInsert = {key, value};
                    KV kvToIgnore, kvToInsertFromLog;
                    if (ll->log.is_delete()) {
                        kvToIgnore = {ll->log.key, ll->log.value};
                        kvToInsertFromLog = {ll->secondLog.key,
                                             ll->secondLog.value};
                    } else {
                        assert(ll->secondLog.is_delete());
                        kvToIgnore = {ll->secondLog.key, ll->secondLog.value};
                        kvToInsertFromLog = {ll->log.key, ll->log.value};
                    }

                    KV kv1, kv2;

                    if (kvToInsertFromLog.key < kvToNewlyInsert.key) {
                        kv1 = kvToInsertFromLog;
                        kv2 = kvToNewlyInsert;
                    } else {
                        kv1 = kvToNewlyInsert;
                        kv2 = kvToInsertFromLog;
                    }

                    int i = 0;
                    int j = 0;

                    while (i < ll->size && ll->key_vals[i].key < kv1.key) {
                        if (ll->key_vals[i].key == kvToIgnore.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    key_vals[j++] = kv1;

                    while (i < ll->size && ll->key_vals[i].key < kv2.key) {
                        if (ll->key_vals[i].key == kvToIgnore.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    key_vals[j++] = kv2;

                    while (i < ll->size) {
                        if (ll->key_vals[i].key == kvToIgnore.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    assert(j == ll->size + 1);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }
            }

            //     if (ll->log.is_delete()) {
            //         if (key != ll->log.key) {
            //             insert_and_exclude_log(ll->key_vals, key_vals,
            //             ll->size, key,
            //                                value, ll->log);
            //         } else {
            //             // Just copy the key_vals from the log leaf
            //             for (int i = 0; i < ll->size; i++) {
            //                 key_vals[i] = ll->key_vals[i];
            //             }
            //         }

            //         // There's ll->size - 1 keys in the log leaf (ll->size kv
            //         pairs minus the log entry)
            //         // Now we insert a new key-value pair
            //         // So the new size is ll->size - 1 + 1 = ll->size
            //         header::size = ll->size;
            //     } else {
            //         insert_and_include_log(ll->key_vals, key_vals, ll->size,
            //         key,
            //                                value, ll->log);

            //         // There's ll->size + 1 keys in the log leaf (ll->size kv
            //         pairs plus the log entry)
            //         // Now we insert a new key-value pair
            //         // So the new size is ll->size + 1 + 1 = ll->size + 2
            //         header::size = ll->size + 2;
            //     }

            assert(this->is_appendable());
            assert(!this->log.is_valid());
            assert(!this->secondLog.is_valid());
            assert(this->kvsAreSorted());
            assert(this->noNullValues());
        }

        // CoW remove a key from log leaf
        // From another log leaf because from a small leaf we won't end up with
        // a log leaf after removal
        generic_log_leaf(log_leaf_ptr ll, K &key)
            : header(ll->key, LogLeaf, ll->size, ll->byte_num) {
            // Remember the header constructor

            assert(!ll->is_appendable());
            assert(ll->log.is_valid());
            assert(ll->secondLog.is_valid());

            if (ll->log.key == ll->secondLog.key) {
                // Should be rare
                // If this happens, the log entries cancel each other out
                assert(ll->log.is_delete() != ll->secondLog.is_delete());

                if (!ll->log.is_delete()) {
                    // If the key in the logs is first inserted then deleted, it
                    // doesn't exist in the KV array It also is definitely not
                    // equal to the key that I'm removing because I wouldn't end
                    // up here
                    assert(ll->log.key != key);
                    // So I should just copy the key_vals from the old log leaf
                    // to the new one And also ignore the key that I'm removing
                    int j = 0;
                    for (int i = 0; i < ll->size; i++) {
                        if (ll->key_vals[i].key == key) { continue; }
                        key_vals[j++] = ll->key_vals[i];
                    }
                    header::size = ll->size - 1;
                    assert(j == ll->size - 1);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // If the key in the logs is first deleted then inserted, it
                    // exists in the KV array It may or may not be equal to the
                    // key that I'm removing

                    if (ll->log.key == key) {
                        // If the key in the logs is equal to the key that I'm
                        // removing, I should just ignore the log entries And
                        // also remove it from the KV array because it doesn't
                        // exist anymore

                        int i = 0;
                        int j = 0;

                        while (i < ll->size) {
                            if (ll->key_vals[i].key == key) {
                                // Clean this up from kv array
                                i++;
                                continue;
                            }
                            key_vals[j] = ll->key_vals[i];
                            j++;
                            i++;
                        }

                        assert(j == ll->size - 1);
                        header::size = ll->size - 1;
                        assert(this->kvsAreSorted());
                        assert(this->noNullValues());
                    } else {
                        // If the key in the logs is not equal to the key that
                        // I'm removing, I should:
                        // 1. Get rid of the key that I'm removing from the kv
                        // array
                        // 2. Update the value for the kv pair that matches the
                        // key in the log entry
                        assert(!ll->secondLog.is_delete());
                        V newValue = ll->secondLog.value;
                        assert(newValue != nullptr);

                        int i = 0;
                        int j = 0;

                        while (i < ll->size) {
                            if (ll->key_vals[i].key == key) {
                                // Clean this up from kv array
                                i++;
                                continue;
                            }
                            key_vals[j] = ll->key_vals[i];
                            if (ll->key_vals[i].key == ll->secondLog.key) {
                                // Update the value for the kv pair that matches
                                // the key in the log entry
                                key_vals[j].value = newValue;
                            }
                            j++;
                            i++;
                        }

                        header::size = ll->size - 1;  // n - 1 + 1 = n
                        assert(j == ll->size - 1);
                        assert(this->kvsAreSorted());
                        assert(this->noNullValues());

                        // KV kvToInsert = {ll->secondLog.key,
                        // ll->secondLog.value}; int i = 0; int j = 0; while (i
                        // < ll->size && ll->key_vals[i].key < kvToInsert.key) {
                        //     if (ll->key_vals[i].key == key) {
                        //         i++;
                        //         continue;
                        //     }
                        //     key_vals[j++] = ll->key_vals[i];
                        //     i++;
                        // }

                        // key_vals[j++] = kvToInsert;

                        // while (i < ll->size) {
                        //     if (ll->key_vals[i].key == key) {
                        //         i++;
                        //         continue;
                        //     }
                        //     key_vals[j++] = ll->key_vals[i];
                        //     i++;
                        // }

                        // header::size = ll->size; // got rid of one, and
                        // insertion-sorted one assert(this->kvsAreSorted());
                        // assert(j == ll->size);
                    }
                }
            } else if ((!ll->log.is_delete()) && (!ll->secondLog.is_delete())) {
                // Both log entries are inserts
                // They are different though
                assert(ll->log.key != ll->secondLog.key);

                if (key == ll->log.key || key == ll->secondLog.key) {
                    // The key that I'm removing has been inserted by some log
                    // entry I should ignore that log entry and insert the other
                    // log entry into the kv array
                    KV kvToInsert;
                    if (key == ll->log.key) {
                        kvToInsert = {ll->secondLog.key, ll->secondLog.value};
                    } else {
                        kvToInsert = {ll->log.key, ll->log.value};
                    }

                    int i = 0;
                    int j = 0;

                    while (i < ll->size &&
                           ll->key_vals[i].key < kvToInsert.key) {
                        assert(ll->key_vals[i].key != key);
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kvToInsert;
                    while (i < ll->size) {
                        assert(ll->key_vals[i].key != key);
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    header::size = ll->size + 1;
                    assert(j == ll->size + 1);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // The key that I'm removing is in some other kv pair in the
                    // array So I should insertion-sort both log entries into
                    // the kv array And get rid of the kv pair that is deleted
                    // by the log entry

                    assert(key != ll->log.key);
                    assert(key != ll->secondLog.key);

                    KV kv1, kv2;

                    if (ll->log.key < ll->secondLog.key) {
                        kv1 = {ll->log.key, ll->log.value};
                        kv2 = {ll->secondLog.key, ll->secondLog.value};
                    } else {
                        kv1 = {ll->secondLog.key, ll->secondLog.value};
                        kv2 = {ll->log.key, ll->log.value};
                    }

                    int i = 0;
                    int j = 0;

                    while (i < ll->size && ll->key_vals[i].key < kv1.key) {
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kv1;
                    while (i < ll->size && ll->key_vals[i].key < kv2.key) {
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kv2;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    header::size = ll->size + 2 - 1;  // TODO
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                    assert(j == ll->size + 2 - 1);
                }
            } else if (ll->log.is_delete() && ll->secondLog.is_delete()) {
                // Both log entries are deletes
                // They are different though
                assert(ll->log.key != ll->secondLog.key);
                // They MUST not be the same as the key that I'm removing
                assert(key != ll->log.key);
                assert(key != ll->secondLog.key);

                int j = 0;

                for (int i = 0; i < ll->size; i++) {
                    if (ll->key_vals[i].key == key) { continue; }
                    if (ll->key_vals[i].key == ll->log.key) { continue; }
                    if (ll->key_vals[i].key == ll->secondLog.key) { continue; }
                    key_vals[j++] = ll->key_vals[i];
                }

                header::size = ll->size - 3;  // n - 2 - 1 = n - 3
                assert(j == ll->size - 3);
                assert(this->kvsAreSorted());
                assert(this->noNullValues());

            } else {
                // One log entry is insert and the other is delete
                // but the keys are not the same
                assert(ll->log.key != ll->secondLog.key);

                if (key != ll->log.key && key != ll->secondLog.key) {
                    // There's i,d or d,i in the logs
                    // But they're not working on the key that I'm removing
                    assert(ll->kvsAreSorted());
                    assert(ll->noNullValues());
                    KV kvToRemoveThisOp = {key, nullptr};
                    KV kvToRemoveFromLog, kvToInsertFromLog;

                    if (ll->log.is_delete()) {
                        kvToRemoveFromLog = {ll->log.key, ll->log.value};
                        kvToInsertFromLog = {ll->secondLog.key,
                                             ll->secondLog.value};
                    } else {
                        assert(ll->secondLog.is_delete());
                        kvToRemoveFromLog = {ll->secondLog.key,
                                             ll->secondLog.value};
                        kvToInsertFromLog = {ll->log.key, ll->log.value};
                    }

                    int i = 0;
                    int j = 0;
                    while (i < ll->size &&
                           ll->key_vals[i].key < kvToInsertFromLog.key) {
                        if (ll->key_vals[i].key == kvToRemoveThisOp.key) {
                            i++;
                            continue;
                        }
                        if (ll->key_vals[i].key == kvToRemoveFromLog.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kvToInsertFromLog;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == kvToRemoveThisOp.key) {
                            i++;
                            continue;
                        }
                        if (ll->key_vals[i].key == kvToRemoveFromLog.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    header::size = ll->size + 1 - 2;  // n - 1 + 1 - 2 = n - 2
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                    assert(j == ll->size + 1 - 2);

                } else {
                    // There's i,d or d,i in the logs
                    // But one of them is working on the key that I'm removing
                    // It can't be both because that case is handled above
                    // I cannot be deleting it in ANY of these log entries,
                    // because the only case where I'd be deleting it is handled
                    // above (both logs working on the same thing)

#ifndef NDEBUG

                    if (ll->log.is_delete()) { assert(key != ll->log.key); }

                    if (ll->secondLog.is_delete()) {
                        assert(key != ll->secondLog.key);
                    }

#endif

                    // I can only be inserting it in one of the logs
                    // So I should just ignore the log entry that's inserting it
                    // And remove the other log entry too from the kv array
                    // while I'm at it

                    KV kvToRemoveFromLog;
                    if (ll->log.is_delete()) {
                        // secondLog is Insert
                        assert(key == ll->secondLog.key);
                        kvToRemoveFromLog = {ll->log.key, ll->log.value};
                    } else {
                        // log is Insert
                        assert(key == ll->log.key);
                        kvToRemoveFromLog = {ll->secondLog.key,
                                             ll->secondLog.value};
                    }

                    int j = 0;
                    for (int i = 0; i < ll->size; i++) {
                        assert(key != ll->key_vals[i].key);
                        if (ll->key_vals[i].key == kvToRemoveFromLog.key) {
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                    }
                    assert(j == ll->size - 1);

                    header::size = ll->size - 1;  // n - 1 - 1 = n - 2
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }
            }

            header::key = key_vals[0].key;
            header::byte_num =
                first_diff(ll->byte_num, &key_vals[0], &key_vals[header::size]);
        }
    };

    template <int MaxSize>
    struct alignas(ALIGNMENT_SIZE) generic_leaf : header {
        using leaf_ptr = generic_leaf<0> *;
        using log_leaf_ptr = generic_log_leaf<max_big_leaf_size> *;
        KV key_vals[MaxSize];

        void print() {
            std::cout << "Leaf: " << std::endl;
            for (int i = 0; i < header::size; i++) {
                std::cout << key_vals[i].key << " ";
            }
            std::cout << std::endl;

            std::cout << "-------------------------------------\n";
        }

        size_t getSumOfKeys() const {
            size_t sum = 0;
            for (int i = 0; i < header::size; i++) { sum += key_vals[i].key; }
            return sum;
        }

        std::vector<K> getKeys() const {  // for debugging
            std::vector<K> keys;
            for (int i = 0; i < header::size; i++) {
                keys.push_back(key_vals[i].key);
            }
            return keys;
        }

        // starts comparing a and b at the start byte (prior bytes are equal)
        // -1 if a < b, 0 if equal and 1 if a > b
        static int cmp(const K &a, const K &b, int start_byte) {
            int la = String::length(a);
            int lb = String::length(b);
            int l = std::min(la, lb);
            int i = start_byte;
            while (i < l && String::get_byte(a, i) == String::get_byte(b, i))
                i++;
            if (i == l) return (la < lb) ? -1 : ((la == lb) ? 0 : 1);
            return (String::get_byte(a, i) < String::get_byte(b, i)) ? -1 : 1;
        }

        std::optional<V> find(const K &k, int byte_pos) {
            // if integer than just search leaf using <.
            if constexpr (std::is_integral_v<K>) {
                for (int i = 0; i < header::size; i++) {
                    if (!(key_vals[i].key < k)) {
                        if (k < key_vals[i].key) {
                            return {};
                        } else {
                            return key_vals[i].value;
                        }
                    }
                }
            } else {
                if (byte_pos < header::byte_num) return {};
                for (int i = 0; i < header::size; i++) {
                    int x = cmp(key_vals[i].key, k, byte_pos);
                    if (x != -1)     // key_vals[i].key >= k
                        if (x == 1)  // key_vals[i].key > k
                            return {};
                        else  // key_vals[i].key == k
                            return key_vals[i].value;
                }
            }
            return {};
        }

        // returns the first byte position where the keys differ
        static int first_diff(int byte_pos, KV *start, KV *end) {
            // base case: only one key
            if (end - start == 1) return String::length(start->key);

            int j = byte_pos;
            while (true) {  // run until you find a differing byte

                // compare j'th byte of all keys in the range

                // j'th byte of the first key
                int byteval = String::get_byte(start->key, j);

                for (KV *ptr = start + 1; ptr < end; ptr++) {
                    // if you reach the end of the key, return j
                    // or
                    // if the j'th byte of the key is different from the
                    // byteval, return j
                    if (j >= String::length(ptr->key) ||
                        String::get_byte(ptr->key, j) != byteval)
                        return j;
                }
                j++;
            }
        }

        inline void flush(const int tid) {
            durableTools::FLUSH_LINES(tid, this, sizeof(generic_leaf<MaxSize>));
        }

        // create singleton leaf
        generic_leaf(K &key, V &value)
            : header(key, Leaf, 1, String::length(key)) {
            key_vals[0] = KV{key, value};
        }

        // create multi leaf
        generic_leaf(int byte_pos, KV *start, KV *end)
            : header(start->key, Leaf, end - start,
                     first_diff(byte_pos, start, end)) {
            for (int i = 0; i < (end - start); i++) key_vals[i] = start[i];
        }

        bool kvsAreSorted() {
            for (int i = 1; i < header::size; i++) {
                if (key_vals[i - 1].key >= key_vals[i].key) { return false; }
            }
            return true;
        }

        bool noNullValues() {
            for (int i = 0; i < header::size; i++) {
                if (key_vals[i].value == nullptr) { return false; }
            }
            return true;
        }

        // static: it doesn't edit the instance
        // like insertion sort but with a twist
        // KV *in is already sorted
        // KV *out is the result of inserting key, value into in
        static void insert(KV *in, KV *out, int n, const K &key,
                           const V &value) {
            int i = 0;
            while (i < n && in[i].key < key) {
                out[i] = in[i];
                i++;
            }
            out[i] = KV{key, value};
            while (i < n) {
                out[i + 1] = in[i];
                i++;
            }

#ifndef NDEBUG

            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    if (out[i].key == out[j].key) {
                        std::cout << "duplicate key at leaf: " << i << ", " << j
                                  << ", " << n << ", " << std::hex << out[i].key
                                  << std::dec << std::endl;
                        assert(false);
                    }
                }
            }

            for (int i = 0; i < n; i++) {
                if (i > 0 && out[i - 1].key >= out[i].key) {
                    std::cout << "out of order at leaf: " << i << ", " << n
                              << ", " << std::hex << out[i - 1].key << ", "
                              << out[i].key << std::dec << std::endl;
                    assert(false);
                }
            }
#endif
        }

        // add to leaf
        generic_leaf(int byte_pos, leaf_ptr l, K &key, V &value)
            : header(l->key, Leaf, l->size + 1, byte_pos) {
            insert(l->key_vals, key_vals, l->size, key, value);
// for (int i = 1; i < header::size; i++)
//         if (!(key_vals[i-1].key < key_vals[i].key)) {
//           std::cout << "x out of order at leaf: " << i << ", " <<
//           header::size << ", " << std::hex <<
//             key_vals[i-1].key << ", " << key_vals[i].key << std::dec <<
//             std::endl;
//           abort();
//         }
#ifndef NDEBUG
            // no duplicates
            for (int i = 0; i < header::size - 1; i++) {
                for (int j = i + 1; j < header::size; j++) {
                    if (key_vals[i].key == key_vals[j].key) { assert(false); }
                }
            }
#endif
        }

        // remove from leaf
        generic_leaf(leaf_ptr l, K &key)
            : header(l->key, Leaf, l->size - 1, 0) {
            int j = 0;
            for (int i = 0; i < l->size; i++) {
                if (l->key_vals[i].key != key) {
                    key_vals[j++] = l->key_vals[i];
                }
            }
            header::key = key_vals[0].key;
            header::byte_num =
                first_diff(l->byte_num, &key_vals[0], &key_vals[header::size]);
#ifndef NDEBUG
            // no duplicates
            for (int i = 0; i < header::size - 1; i++) {
                for (int j = i + 1; j < header::size; j++) {
                    if (key_vals[i].key == key_vals[j].key) { assert(false); }
                }
            }
#endif
        }

        // Remove kv from log leaf and create a new small leaf
        generic_leaf(log_leaf_ptr ll, K &key)
            : header(ll->key, Leaf, ll->size - 1, 0) {
            assert(!ll->is_appendable());
            assert(ll->log.is_valid());
            assert(ll->secondLog.is_valid());
            assert(ll->kvsAreSorted());
            assert(ll->noNullValues());

            if (ll->log.key == ll->secondLog.key) {
                // Should be rare
                // If this happens, the log entries cancel each other out
                // So they can't be both delete or both insert

                assert(ll->log.is_delete() != ll->secondLog.is_delete());

                // std::cout << "Equal log entries: " << ll->log.key << ", " <<
                // ll->secondLog.key << std::endl;

                // Remove the key to remove from the log leaf

                // int j = 0;
                // for (int i = 0; i < ll->size; i++) {
                //     if (ll->key_vals[i].key == key) {
                //         continue;
                //     }
                //     key_vals[j++] = ll->key_vals[i];
                // }

                if (!ll->log.is_delete()) {
                    // std::cout << "First log entry is insert" << std::endl;

                    // If the key in logs is first inserted then deleted, it
                    // doesn't exist in the KV array It also doesn't equal the
                    // key that I'm deleting, because if it did, I wouldn't be
                    // here
                    assert(ll->log.key != key);
                    // I just need to remove the keyToRemove from my kv array
                    header::size = ll->size - 1;

                    int i = 0;
                    int j = 0;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    assert(j == header::size);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // std::cout << "First log entry is delete" << std::endl;
                    // If the key in logs is first deleted then inserted, it
                    // exists in the KV arraya I just need to be careful because
                    // I might have to get rid of two KV pairs in the KV array

                    if (ll->log.key == key) {
                        // std::cout << "First log entry is delete and equal to
                        // key" << std::endl; d, i And it's the key I'm deleting

                        // I'll need to ignore the logs altogether
                        // But remove the kv pair from key-vals
                        header::size = ll->size - 1;

                        int j = 0;
                        for (int i = 0; i < ll->size; i++) {
                            if (ll->key_vals[i].key == key) { continue; }
                            key_vals[j++] = ll->key_vals[i];
                        }

                    } else {
                        // std::cout << "First log entry is delete and not equal
                        // to key" << std::endl; d, i And it's not the key I'm
                        // deleting But it IS in the kv array So I need to
                        // remove the key that I'm deleting from the kv array
                        // And also update the value for the kv pair that
                        // matches the key in the log entry
                        header::size = ll->size - 1;

                        V newValue = ll->secondLog.value;

                        int i = 0;
                        int j = 0;

                        while (i < ll->size) {
                            if (ll->key_vals[i].key == key) {
                                // Clean this up from kv array
                                i++;
                                continue;
                            }
                            key_vals[j] = ll->key_vals[i];
                            if (key_vals[j].key == ll->secondLog.key) {
                                // Update the value for the kv pair that matches
                                // the key in the log entry
                                key_vals[j].value = newValue;
                            }
                            i++;
                            j++;
                        }
                        assert(j == header::size);
                    }

                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }

            } else if ((!ll->log.is_delete()) && (!ll->secondLog.is_delete())) {
                // Both log entries are inserts
                // std::cout <<" Both log entries are inserts" << std::endl;
                assert(ll->log.key != ll->secondLog.key);

                // I may be removing one of the log entries
                // but the other log entry is still valid

                if (ll->log.key == key || ll->secondLog.key == key) {
                    // std::cout << "One of the log entries is equal to the key
                    // that I'm removing" << std::endl;
                    KV logToInsert;
                    if (ll->log.key == key) {
                        logToInsert = {ll->secondLog.key, ll->secondLog.value};
                    } else {
                        logToInsert = {ll->log.key, ll->log.value};
                    }

                    // Insertion-sort the logToInsert into the kv array
                    // And you're good because the kv array doesn't include the
                    // thing you're removing
                    insert(ll->key_vals, key_vals, ll->size, logToInsert.key,
                           logToInsert.value);

                    header::size = ll->size + 1;
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // The key to remove is not a log entry
                    // So we just remove the key from the kv array
                    // std::cout << "The key to remove is not a log entry" <<
                    // std::endl;

                    KV kv1, kv2;
                    if (ll->log.key < ll->secondLog.key) {
                        kv1 = {ll->log.key, ll->log.value};
                        kv2 = {ll->secondLog.key, ll->secondLog.value};
                    } else {
                        kv1 = {ll->secondLog.key, ll->secondLog.value};
                        kv2 = {ll->log.key, ll->log.value};
                    }

                    // Insertion-sort the kv1 and kv2 into the kv array
                    // And make sure to ignore the key to remove
                    K kToRemove = key;

                    int i = 0;
                    int j = 0;

                    while (i < ll->size && ll->key_vals[i].key < kv1.key) {
                        if (ll->key_vals[i].key == kToRemove) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kv1;
                    while (i < ll->size && ll->key_vals[i].key < kv2.key) {
                        if (ll->key_vals[i].key == kToRemove) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    key_vals[j++] = kv2;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == kToRemove) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    header::size = ll->size + 1;
                    assert(j == header::size);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }

            } else if (ll->log.is_delete() && ll->secondLog.is_delete()) {
                // Both log entries are deletes
                // std::cout << "Both log entries are deletes" << std::endl;
                K k1, k2, k3;
                k1 = ll->log.key;
                k2 = ll->secondLog.key;
                k3 = key;

                int j = 0;
                for (int i = 0; i < ll->size; i++) {
                    if (ll->key_vals[i].key == k1 ||
                        ll->key_vals[i].key == k2 ||
                        ll->key_vals[i].key == k3) {
                        continue;
                    }
                    key_vals[j++] = ll->key_vals[i];
                }

                header::size = j;
                assert(j == ll->size - 3);
                assert(this->kvsAreSorted());
                assert(this->noNullValues());
            } else {
                // One log entry is insert and the other is delete
                // but the keys are not the same
                // So I can look at my log entries calmly and be sure that
                // they're operating on different keys Because the edge case has
                // been handled above
                assert(ll->log.key != ll->secondLog.key);
                // std::cout << "One log entry is insert and the other is
                // delete" << std::endl;

                if (!ll->log.is_delete() && ll->log.key == key) {
                    // std::cout << "I am deleting a key that's been inserted by
                    // the first log entry" << std::endl; I am deleting a key
                    // that's been inserted by the first log entry log is insert
                    // secondLog is delete
                    assert(ll->secondLog.is_delete());

                    // The key that I'm deleting is not present in the KV array
                    // (because it's inserted by the log) So I just have to
                    // clean the kv array from the other log entry's delete
                    // record

                    int i = 0;
                    int j = 0;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == ll->secondLog.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    header::size = j;
                    assert(j == ll->size - 1);

                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else if (!ll->secondLog.is_delete() &&
                           ll->secondLog.key == key) {
                    // std::cout << "I am deleting a key that's been inserted by
                    // the second log entry" << std::endl; I am deleting a key
                    // that's been inserted by the second log entry log is
                    // delete
                    assert(ll->log.is_delete());
                    // secondLog is insert

                    // The key that I'm deleting is not present in the KV array
                    // (because it's inserted by the log) So I just have to
                    // clean the kv array from the other log entry's delete
                    // record

                    int i = 0;
                    int j = 0;
                    while (i < ll->size) {
                        if (ll->key_vals[i].key == ll->log.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }
                    header::size = j;
                    assert(j == ll->size - 1);

                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                } else {
                    // std::cout << "I am deleting a key that's not present in
                    // the log entries" << std::endl; The key that I'm deleting
                    // has not been inserted by either log entry So I should
                    // reconcile that with one of the kv pairs that match it I
                    // should also insertion-sort the log entry that's an insert
                    // And ignore the log entry that's a delete
                    // The insert log entry is working on a different key than I
                    // am

                    assert(key != ll->log.key);
                    assert(key != ll->secondLog.key);

                    K keyToRemove = key;
                    KV kvToIgnore, kvToInsertFromLog;
                    if (ll->log.is_delete()) {
                        kvToIgnore = {ll->log.key, ll->log.value};
                        kvToInsertFromLog = {ll->secondLog.key,
                                             ll->secondLog.value};
                    } else {
                        assert(ll->secondLog.is_delete());
                        kvToIgnore = {ll->secondLog.key, ll->secondLog.value};
                        kvToInsertFromLog = {ll->log.key, ll->log.value};
                    }

                    int i = 0;
                    int j = 0;

                    while (i < ll->size &&
                           ll->key_vals[i].key < kvToInsertFromLog.key) {
                        if (ll->key_vals[i].key == keyToRemove ||
                            ll->key_vals[i].key == kvToIgnore.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    key_vals[j++] = kvToInsertFromLog;

                    while (i < ll->size) {
                        if (ll->key_vals[i].key == keyToRemove ||
                            ll->key_vals[i].key == kvToIgnore.key) {
                            i++;
                            continue;
                        }
                        key_vals[j++] = ll->key_vals[i];
                        i++;
                    }

                    header::size = j;
                    assert(j == ll->size - 1);
                    assert(this->kvsAreSorted());
                    assert(this->noNullValues());
                }
            }
            // std::cout << "End of constructor" << std::endl;
            header::key = key_vals[0].key;
            header::byte_num =
                first_diff(ll->byte_num, &key_vals[0], &key_vals[header::size]);
        }
    };

    using leaf = generic_leaf<0>;  // only used generically
    using small_leaf = generic_leaf<max_small_leaf_size>;
    using big_leaf = generic_leaf<max_big_leaf_size>;
    using log_leaf = generic_log_leaf<max_big_leaf_size>;

    static verlib::memory_pool<full_node> full_pool;
    static verlib::memory_pool<indirect_node> indirect_pool;
    static verlib::memory_pool<sparse_node> sparse_pool;
    static verlib::memory_pool<small_leaf> small_leaf_pool;
    static verlib::memory_pool<big_leaf> big_leaf_pool;
    static verlib::memory_pool<log_leaf> log_leaf_pool;

    void initThread(const int tid) {
        verlib::setbench_tid = tid;
        flck::setbench_tid = tid;
    }

    // dispatch based on node type
    // A returned nullptr means no child matching the key
    static inline node_ptr *get_child(node *x, const K &k) {
        switch (x->nt) {
            case Full: return ((full_node *)x)->get_child(k);
            case Indirect: return ((indirect_node *)x)->get_child(k);
            case Sparse: return ((sparse_node *)x)->get_child(k);
        }
        return nullptr;
    }

    // dispatch based on node type
    static inline bool is_full(node *p) {
        switch (p->nt) {
            case Full: return ((full_node *)p)->is_full();  // never full
            case Indirect: return ((indirect_node *)p)->is_full();
            case Sparse: return ((sparse_node *)p)->is_full();
        }
        return false;
    }

    // Adds a new child to p with key k and value v
    // gp is p's parent (i.e. grandparent)
    // This involve copying p and updating gp to point to it.
    // This should never be called on a full node.
    // Returns false if it fails.
    static bool add_child(const int tid, node *gp, node *p, const K &k,
                          const V &v) {
        ART_GSTATS_ADD(tid, add_child_codepath_root, 1);
        return GET_LOCK(gp).try_lock(tid, [=] {
            auto child_ptr = get_child(gp, p->key);
            bool rm = gp->removed.load();
            bool pt = child_ptr->load() != p;
            if (gp->removed.load() || child_ptr->load() != p) return false;
            return GET_LOCK(p).try_lock(tid, [=] {
                if (get_child(p, k) != nullptr) return false;

                /*
                NODE CREATION POINT:
                A new leaf is created, holding:
                Only the new key-value pair to be inserted
                */
                small_leaf *c = small_leaf_pool.new_obj(tid, k, v);

                /*
                NEW NODE FLUSH POINT
                */
                // TODO: This flush might be better off where c is actually
                // linked in a new indirect/sparse/full node There is quite a
                // distance between the two points. Parallel flushes might be
                // beneficial?
                c->flush(tid);

                if (p->nt == Indirect) {
                    indirect_node *i_n = (indirect_node *)p;

                    /*
                    WRITE POINT:
                    The removed flag is set to true, indicating that the
                    indirect node is no longer in use CAREFUL: This is the only
                    place where we edit a field of a node in-place Other threads
                    might be reading this removed value (NOT reader threads in
                    case of lock-free reads)
                    */
                    i_n->removed = true;

                    /*
                    TODO: POTENTIAL FLUSH POINT?
                    */

                    if (is_full(p)) {
                        // copy indirect to full
                        ART_GSTATS_ADD(tid, add_child_codepath_cow_i2f, 1);

                        /*
                        NODE CREATION POINT:
                        A new full node is created, holding:
                        Whatever was in the indirect node, plus the new leaf
                        containing the key-value pair to be inserted
                        */
                        full_node *new_f =
                            full_pool.new_init(tid, [=](full_node *f_n) {

#ifdef OrderedIndirectNodes
                                f_n->key = i_n->key;
                                f_n->byte_num = i_n->byte_num;
                                int idx = 0;
                                for (int i = 0; i < 256; i++) {
                                    auto [which_word, which_bit] =
                                        i_n->word_idx_and_bit(i);
                                    auto mask = 1ULL << which_bit;
                                    if (i_n->bitmap[which_word] & mask) {
                                        f_n->children[i].init(
                                            i_n->ptr[idx++].load());
                                    }
                                }

                                auto b = String::get_byte(k, f_n->byte_num);
                                f_n->children[b].init((node *)c);
#else 
                            f_n->key = i_n->key;
                            f_n->byte_num = i_n->byte_num;
                            for (int i = 0; i < 256; i++) {
                                int j = i_n->idx[i];
                                if (j != -1) f_n->children[i].init(i_n->ptr[j].load());
                            }
                            // seemingly broken g++ (10.3) compiler makes the following fail
                            //   f_n->init_child(k, c);
                            // inlining by hand fixes it.
                            auto b = String::get_byte(k, f_n->byte_num);
                            f_n->children[b].init((node *)c);
#endif
                            });

                        /*
                        NEW NODE FLUSH POINT
                        CAREFUL: There might be implied fences in init()
                        functions that set the ptrs
                        */
                        new_f->flush(tid);

                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new full
                        node
                        */

                        *child_ptr = (node *)new_f;

                    } else {
                        // copy indirect to indirect
                        ART_GSTATS_ADD(tid, add_child_codepath_cow_i2i, 1);

                        /*
                        NODE CREATION POINT:
                        A new indirect node is created, holding:
                        Whatever was in the indirect node, plus the new leaf
                        containing the key-value pair to be inserted
                        */
                        indirect_node *new_i = indirect_pool.new_init(
                            tid, [=](indirect_node *i_c) {

#ifdef OrderedIndirectNodes
                                i_c->key = i_n->key;
                                i_c->byte_num = i_n->byte_num;
                                i_c->size = i_n->size + 1;

                                // we're inserting new key-value pair in sorted
                                // order
                                auto byteval =
                                    String::get_byte(k, i_c->byte_num);

                                assert(i_n->size == i_n->total_popcount());

                                // first set the bits in the bitmap
                                for (int i = 0; i < 256; i++) {
                                    auto [which_word, which_bit] =
                                        i_n->word_idx_and_bit(i);
                                    auto mask = 1ULL << which_bit;
                                    if (i_n->bitmap[which_word] & mask) {
                                        i_c->bitmap[which_word] |= mask;
                                    }
                                }

                                auto [which_word, which_bit] =
                                    i_c->word_idx_and_bit(byteval);
                                auto final_ptr_idx =
                                    i_c->ones_to_left(which_word, which_bit);
                                auto mask = 1ULL << which_bit;
                                i_c->bitmap[which_word] |= mask;

                                int idx_in_new = 0;
                                int idx_in_old = 0;
                                for (int i = 0; i < 256; i++) {
                                    auto [which_word, which_bit] =
                                        i_n->word_idx_and_bit(i);
                                    auto mask = 1ULL << which_bit;
                                    if (i_n->bitmap[which_word] & mask) {
                                        if (final_ptr_idx == idx_in_old) {
                                            idx_in_new++;
                                        }
                                        assert(abs(idx_in_new - idx_in_old) <
                                               2);
                                        i_c->ptr[idx_in_new++].init(
                                            i_n->ptr[idx_in_old++].load());
                                    }
                                }

                                i_c->ptr[final_ptr_idx].init((node *)c);

                                assert(i_c->total_popcount() == i_c->size);
#else 
                            i_c->key = i_n->key;
                            i_c->byte_num = i_n->byte_num;
                            i_c->size = i_n->size + 1;
                            for (int i = 0; i < 256; i++) i_c->idx[i] = i_n->idx[i];
                            for (int i = 0; i < i_n->size; i++) i_c->ptr[i].init(i_n->ptr[i].load());
                            i_c->init_child(k, (node *)c);
#endif
                            });

                        /*
                        NEW NODE FLUSH POINT
                        CAREFUL: There might be implied fences in init()
                        functions that set the ptrs
                        */
                        new_i->flush(tid);
                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new
                        indirect node
                        */
                        *child_ptr = (node *)new_i;
                    }
                    indirect_pool.retire(tid, i_n);
                } else {  // (p->nt == Sparse)
                    sparse_node *s_n = (sparse_node *)p;

                    /*
                    WRITE POINT:
                    The removed flag is set to true, indicating that the sparse
                    node is no longer in use CAREFUL: This is the only place
                    where we edit a field of a node in-place Other threads might
                    be reading this removed value (NOT reader threads in case of
                    lock-free reads)
                    */
                    s_n->removed = true;

                    /*
                    TODO: POTENTIAL FLUSH POINT?
                    */

                    if (is_full(p)) {
                        // copy sparse to indirect
                        ART_GSTATS_ADD(tid, add_child_codepath_cow_s2i, 1);

                        /*
                        NODE CREATION POINT:
                        A new indirect node is created, holding:
                        Whatever was in the sparse node, plus the new leaf
                        containing the key-value pair to be inserted
                        */

                        indirect_node *new_i = indirect_pool.new_init(
                            tid, [=](indirect_node *i_n) {

#ifdef OrderedIndirectNodes
                                assert(s_n->size <= max_sparse_size);
                                i_n->key = s_n->key;
                                i_n->byte_num = s_n->byte_num;
                                i_n->size = max_sparse_size + 1;

                                // first set the bits in the bitmap

                                // pre-existing keys from the sparse node
                                for (int i = 0; i < max_sparse_size; i++) {
                                    auto [which_word, which_bit] =
                                        i_n->word_idx_and_bit(s_n->keys[i]);
                                    auto mask = 1ULL << which_bit;
                                    assert(!(
                                        i_n->bitmap[which_word] &
                                        mask));  // key should not already exist
                                    i_n->bitmap[which_word] |= mask;
                                }
                                assert(i_n->total_popcount() == s_n->size);

                                // new key
                                auto new_idx =
                                    String::get_byte(k, i_n->byte_num);
                                auto [which_word, which_bit] =
                                    i_n->word_idx_and_bit(new_idx);
                                auto mask = 1ULL << which_bit;

                                assert(
                                    !(i_n->bitmap[which_word] &
                                      mask));  // key should not already exist
                                i_n->bitmap[which_word] |= mask;

                                assert(i_n->total_popcount() == i_n->size);

                                auto final_ptr_idx =
                                    i_n->ones_to_left(which_word, which_bit);

                                for (int j = 0; j < max_sparse_size; j++) {
                                    auto [which_word, which_bit] =
                                        i_n->word_idx_and_bit(s_n->keys[j]);
                                    auto ptr_idx = i_n->ones_to_left(which_word,
                                                                     which_bit);

                                    assert(i_n->bitmap[which_word] &
                                           (1ULL << which_bit));
                                    assert(ptr_idx != final_ptr_idx);
                                    assert(ptr_idx < i_n->size && ptr_idx >= 0);

                                    i_n->ptr[ptr_idx].init(s_n->ptr[j].load());
                                }
                                i_n->ptr[final_ptr_idx].init((node *)c);

                                assert(i_n->total_popcount() == i_n->size);

#else 
                            assert(s_n->size <= max_sparse_size);
                            i_n->key = s_n->key;
                            i_n->byte_num = s_n->byte_num;
                            i_n->size = max_sparse_size + 1;
                            for (int i = 0; i < 256; i++) i_n->idx[i] = -1;
                            for (int i = 0; i < max_sparse_size; i++) {
                                // there should be no duplicates 
                                assert(i_n->idx[s_n->keys[i]] == -1);
                                i_n->idx[s_n->keys[i]] = i;
                                i_n->ptr[i].init(s_n->ptr[i].load());
                            }
                            i_n->init_child(k, (node *)c);
#endif
                            });

                        /*
                        NEW NODE FLUSH POINT
                        CAREFUL: There might be implied fences in init()
                        functions that set the ptrs
                        */
                        new_i->flush(tid);

                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new
                        indirect node
                        */
                        *child_ptr = (node *)new_i;

                    } else {
                        // copy sparse to sparse
                        ART_GSTATS_ADD(tid, add_child_codepath_cow_s2s, 1);

                        /*
                        NODE CREATION POINT:
                        A new sparse node is created, holding:
                        Whatever was in the sparse node, plus the new leaf
                        containing the key-value pair to be inserted
                        */

                        sparse_node *new_s =
                            sparse_pool.new_init(tid, [=](sparse_node *s_c) {
                                s_c->key = s_n->key;
                                s_c->byte_num = s_n->byte_num;
                                s_c->size = s_n->size + 1;
                                for (int i = 0; i < s_n->size; i++) {
                                    s_c->keys[i] = s_n->keys[i];
                                    s_c->ptr[i].init(s_n->ptr[i].load());
                                }
                                s_c->init_child(k, (node *)c);
                            });

                        assert(new_s->size <= max_sparse_size);

                        /*
                        NEW NODE FLUSH POINT
                        CAREFUL: There might be implied fences in init()
                        functions that set the ptrs
                        */
                        new_s->flush(tid);

                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new sparse
                        node
                        */
                        *child_ptr = (node *)new_s;
                    }
                    sparse_pool.retire(tid, s_n);
                }
                return true;
            });  // end try_lock(GET_LOCK(p)
            return true;
        });  // end try_lock(GET_LOCK(gp)
    }

    //*** will return one of 4 things: no child, empty child, leaf, subtree (cut
    // edge)
    static auto find_location(const int tid, node *root, const K &k) {
        int byte_pos = 0;
        node *gp = nullptr;
        node *p = root;
        while (true) {
            node_ptr *cptr = get_child(p, k);
            if (cptr == nullptr)  // has no child with given key
                return std::make_tuple(gp, p, cptr, (node *)nullptr, byte_pos);
            // could be read()
            node *c = cptr->load();

            if (c == nullptr)  // has empty child with given key
                return std::make_tuple(gp, p, cptr, c, byte_pos);

            // increment the byte_pos at least once
            byte_pos++;

            // now increment byte_pos until you find a differing byte
            // stop if you reach the end of the key (the first condition in the
            // while loop)
            while (byte_pos < c->byte_num &&
                   String::get_byte(k, byte_pos) ==
                       String::get_byte(c->key, byte_pos)) {
                byte_pos++;
            }

            // TODO: what does byte_pos != c->byte_num mean? study insert more
            // carefully.

            // if byte_pos != c->byte_num, then the second condition in the
            // while loop terminated the loop, meaning that the keys differ at
            // byte_pos
            if (byte_pos != c->byte_num || c->is_leaf())
                return std::make_tuple(gp, p, cptr, c, byte_pos);

            gp = p;
            p = c;
        }
    }

    static std::pair<void *, bool> new_leaf(const int tid, int byte_pos,
                                            KV *start, KV *end) {
        // If we have log leaves with only one log entry, there's only two types
        // of leaves: log leaves and small leaves Log leaves are actually big
        // leaves with a log entry followed by max_big_leaf_size key-value pairs

        void *resNode = nullptr;

        if ((end - start) > max_small_leaf_size)
            resNode = (void *)log_leaf_pool.new_obj(tid, byte_pos, start, end);
        else
            resNode =
                (void *)small_leaf_pool.new_obj(tid, byte_pos, start, end);

        return {resNode, (end - start) > max_small_leaf_size};
    }

    static bool differenceIs(std::vector<K> &before, std::vector<K> &after,
                             K key, bool isInsert) {
        std::set<K> beforeSet(before.begin(), before.end());
        std::set<K> afterSet(after.begin(), after.end());

        if (isInsert) {
            // The set difference should be key
            std::set<K> diff;
            std::set_difference(afterSet.begin(), afterSet.end(),
                                beforeSet.begin(), beforeSet.end(),
                                std::inserter(diff, diff.begin()));
            if (diff.size() != 1) { return false; }
            if (*diff.begin() != key) { return false; }
            return true;
        } else {
            std::set<K> diff;
            std::set_difference(beforeSet.begin(), beforeSet.end(),
                                afterSet.begin(), afterSet.end(),
                                std::inserter(diff, diff.begin()));
            if (diff.size() != 1) { return false; }
            if (*diff.begin() != key) { return false; }
            return true;
        }

        return true;
    }

    V insert_(const int tid, const K &k, const V &v) {
        V ret = flck::try_loop([&]() {
            return try_insert(tid, k, v);
        });
        return ret;
    }

    V insert(const int tid, const K &k, const V &v) {
        #ifdef MEASURE_COW_TIME 
        long start;
        if (verlib::measuring_cow_time) {
            start = art_rdtsc();
        }
        #endif 
        auto ret = verlib::with_epoch(tid, [=] { return insert_(tid, k, v); });
        #ifdef MEASURE_COW_TIME
        if (verlib::measuring_cow_time && ret == NO_VALUE) {
            auto end = art_rdtsc();
            auto duration = end - start;
            GSTATS_ADD(tid, insert_time_root, duration);
            GSTATS_ADD(tid, inserts_timed, 1);
        }
        #endif 

        return ret;
    }

    std::optional<V> try_insert(const int tid, const K &k, const V &v,
                                bool upsert = false) {
        ART_GSTATS_ADD(tid, insert_codepath_root, 1);
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        if (c != nullptr && c->is_leaf() &&
            (c->nt == LogLeaf ? (((log_leaf *)c)->find(k, byte_pos).has_value())
                              : (((leaf *)c)->find(k, byte_pos).has_value()))) {
            if (GET_LOCK(p).read_lock([&] {
                    return (cptr->load() == c) && !p->removed.load();
                })) {
                auto ret =
                    (c->nt == LogLeaf ? ((log_leaf *)c)->find(k, byte_pos)
                                      : ((leaf *)c)->find(k, byte_pos));
                ART_GSTATS_ADD(tid, insert_codepath_key_exists, 1);
                return ret;
            } else {
                ART_GSTATS_ADD(tid, insert_codepath_retry, 1);
                return {};
            }
        }

        if (cptr !=
            nullptr) {  // child pointer exists, always true for full node
            if (GET_LOCK(p).try_lock(tid, [=] {
                    // exit and retry if state has changed
                    if (p->removed.load() || cptr->load() != c) {
                        ART_GSTATS_ADD(tid, insert_codepath_retry_after_lock, 1);
                        return false;
                    }

                    // The state also might have changed if a key was inserted
                    // by logs (in the case of log leaves)
                    if (c != nullptr && c->is_leaf() && c->nt == LogLeaf) {
                        log_leaf *ll = (log_leaf *)c;
                        if (ll->find(k, byte_pos).has_value()) {
                            ART_GSTATS_ADD(tid, insert_codepath_rare_case_key_found,
                                       1);
                            return false;
                        }
                    }

                    // fill a null pointer with the new leaf
                    if (c == nullptr) {
                        ART_GSTATS_ADD(tid, insert_codepath_add_to_empty, 1);
                        #ifdef MEASURE_COW_TIME
                        long start;
                        if (verlib::measuring_cow_time) {
                            start = art_rdtsc();
                        }
                        #endif 

                        /*
                        NODE CREATION POINT:
                           A new small leaf is created, holding:
                             Only the new key-value pair to be inserted
                        */
                        small_leaf *new_l = small_leaf_pool.new_obj(tid, k, v);

                        
                        new_l->flush(tid);
                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new small
                        leaf.
                        */
                        *cptr = (node *)new_l;
                        #ifdef MEASURE_COW_TIME
                        if (verlib::measuring_cow_time) {
                            auto end = art_rdtsc();
                            auto duration = end - start;
                            GSTATS_ADD(tid, insert_time_new_node, duration);
                        }
                        #endif

#ifndef NDEBUG
                        auto keysAfter = new_l->getKeys();
                        assert(keysAfter.size() == 1);
                        assert(keysAfter.at(0) == k);
#endif

                        assert(k == new_l->getSumOfKeys());

                    } else if (c->is_leaf()) {
                        if (c->nt == LogLeaf) {
                            // Possibly Appendable
                            log_leaf *ll = (log_leaf *)c;

#ifndef NDEBUG
                            auto keysBefore = ll->getKeys();
#endif

                            int realSize = ll->size;
                            if (ll->log.is_valid()) {
                                if (ll->log.is_delete()) {
                                    realSize--;
                                } else {
                                    realSize++;
                                }
                            }
                            if (ll->secondLog.is_valid()) {
                                if (ll->secondLog.is_delete()) {
                                    realSize--;
                                } else {
                                    realSize++;
                                }
                            }

                            int newSize = realSize + 1;

                            assert(ll->kvsAreSorted());
                            assert(ll->noNullValues());
                            if (ll->is_appendable()) {
                                // Append to log leaf
                                ART_GSTATS_ADD(tid, insert_codepath_append_log, 1);

#ifndef NDEBUG
                                size_t oldSum = ll->getSumOfKeys();
#endif

                                ll->append(tid, k, v, false);

                                assert(ll->find(k, byte_pos).has_value());
                                assert(ll->kvsAreSorted());
                                assert(ll->noNullValues());
#ifndef NDEBUG
                                size_t newSum = ll->getSumOfKeys();
                                assert(oldSum + k == newSum);
                                auto keysAfter = ll->getKeys();
                                assert(keysAfter.size() ==
                                       keysBefore.size() + 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    true));
#endif

                                return true;
                            } else {
                                // Dealing with a log leaf that doesn't have
                                // space for a new log entry We'll either CoW
                                // into a new log leaf, or if there's no room,
                                // we'll CoW into a sparse node with a bunch of
                                // children

                                assert(ll->log.is_valid());
                                assert(ll->secondLog.is_valid());

                                if (realSize < max_big_leaf_size) {
                                    // Fits into a log leaf
                                    // CoW into a new log leaf that has room for
                                    // a new log entry

                                    ART_GSTATS_ADD(tid, insert_codepath_cow_b2b, 1);

#ifndef NDEBUG
                                    size_t oldSum = ll->getSumOfKeys();
#endif
                                    
                                    #ifdef MEASURE_COW_TIME
                                    long start; 
                                    if (verlib::measuring_cow_time) {
                                        start = art_rdtsc();
                                    }
                                    #endif
                                    log_leaf *new_ll = log_leaf_pool.new_obj(
                                        tid, byte_pos, ll, k, v);
                                    
                                    assert(new_ll->is_appendable());
                                    assert(!new_ll->log.is_valid());
                                    assert(!new_ll->secondLog.is_valid());
                                    // assert(new_ll->size == realSize + 1);
                                    assert(new_ll->kvsAreSorted());
                                    assert(new_ll->noNullValues());
                                    assert(
                                        new_ll->find(k, byte_pos).has_value());
                                    assert(new_ll->size == realSize + 1);
                                    assert(new_ll->size <= max_big_leaf_size);

                                    new_ll->flush(tid);

                                    *cptr = (node *)new_ll;

                                    #ifdef MEASURE_COW_TIME
                                    if (verlib::measuring_cow_time) {
                                        auto end = art_rdtsc();
                                        auto duration = end - start;
                                        GSTATS_ADD(tid, insert_time_leaf_cow, duration);
                                    }
                                    #endif

                                    log_leaf_pool.retire(tid, ll);

#ifndef NDEBUG
                                    size_t newSum = new_ll->getSumOfKeys();
                                    assert(oldSum + k == newSum);

                                    auto keysAfter = new_ll->getKeys();
                                    assert(keysAfter.size() ==
                                           keysBefore.size() + 1);
                                    assert(differenceIs(keysBefore, keysAfter,
                                                        k, true));
#endif

                                    return true;

                                } else {
                                    // CoW log leaf into a new sparse node with
                                    // a bunch of children
                                    ART_GSTATS_ADD(tid,
                                               insert_codepath_cow_b2sparse, 1);

                                    assert(realSize >= max_big_leaf_size);
                                    assert(realSize <= max_big_leaf_size + 2);

#ifndef NDEBUG
                                    size_t oldSum = ll->getSumOfKeys();
                                    size_t newSum = 0;

#endif

                                    int n = newSize;

                                    #ifdef MEASURE_COW_TIME
                                    long startTime;
                                    if (verlib::measuring_cow_time) {
                                        startTime = art_rdtsc();
                                    }
                                    #endif

                                    std::vector<KV> tmpVec;
                                    tmpVec.reserve(n);
                                    for (int i = 0; i < ll->size; i++) {
                                        tmpVec.push_back(ll->key_vals[i]);
                                    }

                                    if (ll->log.is_delete()) {
                                        tmpVec.erase(
                                            std::remove_if(
                                                tmpVec.begin(), tmpVec.end(),
                                                [&](const KV &kv) {
                                                    return kv.key ==
                                                           ll->log.key;
                                                }),
                                            tmpVec.end());
                                    } else {
                                        tmpVec.push_back(
                                            {ll->log.key, ll->log.value});
                                    }

                                    if (ll->secondLog.is_delete()) {
                                        tmpVec.erase(
                                            std::remove_if(
                                                tmpVec.begin(), tmpVec.end(),
                                                [&](const KV &kv) {
                                                    return kv.key ==
                                                           ll->secondLog.key;
                                                }),
                                            tmpVec.end());
                                    } else {
                                        tmpVec.push_back({ll->secondLog.key,
                                                          ll->secondLog.value});
                                    }

                                    tmpVec.push_back({k, v});

                                    std::sort(tmpVec.begin(), tmpVec.end(),
                                              [](const KV &a, const KV &b) {
                                                  return a.key < b.key;
                                              });

                                    KV *tmp = tmpVec.data();
                                    assert(tmpVec.size() == n);

#ifndef NDEBUG

                                    for (int i = 1; i < n; i++) {
                                        assert(tmp[i - 1].key < tmp[i].key);
                                    }

                                    // Assert there's no duplicates
                                    for (int i = 0; i < n; i++) {
                                        for (int j = i + 1; j < n; j++) {
                                            assert(tmp[i].key != tmp[j].key);
                                        }
                                    }

#endif

                                    auto gb = [&](const KV &kv) {
                                        return String::get_byte(kv.key,
                                                                byte_pos);
                                    };

                                    int j = 0;
                                    int start = 0;
                                    int byteval = gb(tmp[0]);

                                    node *children[n];

                                    for (int i = 1; i < n; i++) {
                                        if (gb(tmp[i]) != byteval) {
                                            auto [new_l, is_log_leaf] =
                                                new_leaf(tid, byte_pos + 1,
                                                         &tmp[start], &tmp[i]);

                                            if (is_log_leaf) {
                                                assert(((log_leaf *)new_l)
                                                           ->kvsAreSorted());
                                                assert(((log_leaf *)new_l)
                                                           ->noNullValues());
                                                ((log_leaf *)new_l)->flush(tid);

#ifndef NDEBUG
                                                newSum += ((log_leaf *)new_l)
                                                              ->getSumOfKeys();
#endif
                                            } else {
                                                assert(((small_leaf *)new_l)
                                                           ->kvsAreSorted());
                                                assert(((small_leaf *)new_l)
                                                           ->noNullValues());
                                                ((small_leaf *)new_l)
                                                    ->flush(tid);

#ifndef NDEBUG
                                                newSum += ((small_leaf *)new_l)
                                                              ->getSumOfKeys();
#endif
                                            }

                                            children[j++] = (node *)new_l;
                                            start = i;
                                            byteval = gb(tmp[i]);
                                        }
                                    }

                                    auto [last_new_l, is_log_leaf] =
                                        new_leaf(tid, byte_pos + 1, &tmp[start],
                                                 &tmp[n]);

                                    if (is_log_leaf) {
                                        assert(((log_leaf *)last_new_l)
                                                   ->kvsAreSorted());
                                        assert(((log_leaf *)last_new_l)
                                                   ->noNullValues());
                                        ((log_leaf *)last_new_l)->flush(tid);

#ifndef NDEBUG
                                        newSum += ((log_leaf *)last_new_l)
                                                      ->getSumOfKeys();
#endif
                                    } else {
                                        assert(((small_leaf *)last_new_l)
                                                   ->kvsAreSorted());
                                        assert(((small_leaf *)last_new_l)
                                                   ->noNullValues());
                                        ((small_leaf *)last_new_l)->flush(tid);

#ifndef NDEBUG
                                        newSum += ((small_leaf *)last_new_l)
                                                      ->getSumOfKeys();
#endif
                                    }

                                    children[j++] = (node *)last_new_l;

                                    sparse_node *new_s = sparse_pool.new_obj(
                                        tid, byte_pos, (node **)&children[0],
                                        (node **)&children[j]);

                                    
                                    assert(new_s->size <= max_sparse_size);
                                    assert(oldSum + k == newSum);

                                    new_s->flush(tid);

                                    *cptr = (node *)new_s;

                                    #ifdef MEASURE_COW_TIME
                                    if (verlib::measuring_cow_time) {
                                        auto endTime = art_rdtsc();
                                        auto duration = endTime - startTime;
                                        GSTATS_ADD(tid, insert_time_split_cow, duration);
                                    }
                                    #endif


                                    log_leaf_pool.retire(tid, ll);
                                    return true;
                                }

                                // TODO: what if realSize is equal to
                                // max_big_leaf_size + 1? Can happen in rare
                                // cases. Will it cause problems? Gotta check
                            }
                        } else {
                            // Small leaf
                            assert(c->nt == Leaf);

                            leaf *l = (leaf *)c;
                            small_leaf *sl = (small_leaf *)c;
                            assert(sl->size <= max_small_leaf_size);

#ifndef NDEBUG
                            auto keysBefore = sl->getKeys();
#endif

                            if (sl->size < max_small_leaf_size) {
                                // CoW sl into a new sl
                                ART_GSTATS_ADD(tid, insert_codepath_cow_s2s, 1);

#ifndef NDEBUG
                                size_t oldSum = sl->getSumOfKeys();
#endif
                                #ifdef MEASURE_COW_TIME
                                long start;
                                if (verlib::measuring_cow_time) {
                                    start = art_rdtsc();
                                }
                                #endif
                                small_leaf *new_sl = small_leaf_pool.new_obj(
                                    tid, byte_pos, l, k, v);
                                

#ifndef NDEBUG
                                assert(oldSum + k == new_sl->getSumOfKeys());
                                auto keysAfter = new_sl->getKeys();
                                assert(keysAfter.size() ==
                                       keysBefore.size() + 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    true));
#endif

                                assert(new_sl->size == sl->size + 1);

                                new_sl->flush(tid);

                                assert(new_sl->size == sl->size + 1);
                                assert(new_sl->find(k, byte_pos).has_value());



                                *cptr = (node *)new_sl;

                                #ifdef MEASURE_COW_TIME
                                if (verlib::measuring_cow_time) {
                                    auto end = art_rdtsc();
                                    auto duration = end - start;
                                    GSTATS_ADD(tid, insert_time_leaf_cow, duration);
                                }
                                #endif

                                small_leaf_pool.retire(tid, sl);
                                return true;
                            } else {
                                // CoW sl into a new ll
                                ART_GSTATS_ADD(tid, insert_codepath_cow_s2b, 1);

#ifndef NDEBUG
                                size_t oldSum = sl->getSumOfKeys();
#endif

#ifdef MEASURE_COW_TIME
                                long start; 
                                if (verlib::measuring_cow_time) {
                                    start = art_rdtsc();
                                }
#endif
                                log_leaf *new_ll = log_leaf_pool.new_obj(
                                    tid, byte_pos, l, k, v);

                               
                                assert(new_ll->kvsAreSorted());
                                assert(new_ll->noNullValues());

#ifndef NDEBUG
                                assert(oldSum + k == new_ll->getSumOfKeys());
                                auto keysAfter = new_ll->getKeys();
                                assert(keysAfter.size() ==
                                       keysBefore.size() + 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    true));
#endif

                                new_ll->flush(tid);

                                assert(new_ll->size == sl->size + 1);
                                assert(new_ll->find(k, byte_pos).has_value());
                                assert(new_ll->is_appendable());

                                *cptr = (node *)new_ll;

                                #ifdef MEASURE_COW_TIME
                                if (verlib::measuring_cow_time) {
                                    auto end = art_rdtsc();
                                    auto duration = end - start;
                                    GSTATS_ADD(tid, insert_time_leaf_cow, duration);
                                }
                                #endif

                                small_leaf_pool.retire(tid, sl);

                                return true;
                            }
                        }

                    } else {  // not a leaf
                        ART_GSTATS_ADD(tid, insert_codepath_add_to_nonleaf, 1);
                        #ifdef MEASURE_COW_TIME
                        long start; 
                        if (verlib::measuring_cow_time) {
                            start = art_rdtsc();
                        }
                        #endif

                        small_leaf *new_l = small_leaf_pool.new_obj(tid, k, v);



                        new_l->flush(tid);

                        sparse_node *new_s = sparse_pool.new_obj(
                            tid, byte_pos, c, c->key, (node *)new_l, k);

                        new_s->flush(tid);

                        *cptr = (node *)new_s;

                        #ifdef MEASURE_COW_TIME
                        if (verlib::measuring_cow_time) {
                            auto end = art_rdtsc();
                            auto duration = end - start;
                            GSTATS_ADD(tid, insert_time_new_node, duration);
                        }
                        #endif
                    }
                    return true;
                })) {
                return NO_VALUE;
            }
        } else {  // no child pointer, need to add
            ART_GSTATS_ADD(tid, insert_codepath_add_child, 1);
            #ifdef MEASURE_COW_TIME
            long start; 
            if (verlib::measuring_cow_time) {
                start = art_rdtsc();
            }
            #endif
            bool x = add_child(tid, gp, p, k, v);
            #ifdef MEASURE_COW_TIME
            if (verlib::measuring_cow_time) {
                auto end = art_rdtsc();
                auto duration = end - start;
                if (x)
                    GSTATS_ADD(tid, insert_time_new_node, duration);
            }
            #endif
            if (x) return NO_VALUE;
        }
        return {};
    }

    // returns other child if node is sparse and has two children, one
    // of which is c, otherwise returns nullptr
    static node *single_other_child(node *p, node *c) {
        if (p->nt != Sparse) return nullptr;
        sparse_node *ps = (sparse_node *)p;
        node *result = nullptr;
        for (int i = 0; i < ps->size; i++) {
            node *oc = ps->ptr[i].load();
            if (oc != nullptr && oc != c)
                if (result != nullptr)
                    return nullptr;  // quit if second child
                else
                    result = oc;  // set first child
        }
        return result;
    }

    // currently a "lazy" remove that only removes
    //   1) the leaf
    //   2) its parent if it is sparse with just two children

    V remove_(const int tid, const K &k) {
        // int attempts = 0;
        V ret = flck::try_loop([&]() {
            // attempts++;
            return try_remove(tid, k);
        });

        // if (attempts > 100) attempts = 100;
        // ART_GSTATS_ADD_IX(tid, delete_attempts_histogram, 1, attempts);
        return ret;
    }

    V remove(const int tid, const K &k) {
        return verlib::with_epoch(tid, [=] { return remove_(tid, k); });
    }

    std::optional<V> try_remove(const int tid, const K &k) {
        ART_GSTATS_ADD(tid, remove_codepath_root, 1);
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        if (cptr == nullptr) {  // TODO: is this correct?
            ART_GSTATS_ADD(tid, remove_codepath_no_child, 1);
            return NO_VALUE;
        }
        // if not found return
        if (c == nullptr ||
            !(c->is_leaf() &&
              (c->nt == LogLeaf
                   ? ((log_leaf *)c)->find(k, byte_pos).has_value()
                   : ((leaf *)c)->find(k, byte_pos).has_value()))) {
            if (GET_LOCK(p).read_lock([&] {
                    return (cptr->load() == c) && !p->removed.load();
                })) {
                ART_GSTATS_ADD(tid, remove_codepath_key_not_found, 1);
                return NO_VALUE;
            } else {
                ART_GSTATS_ADD(tid, remove_codepath_retry, 1);
                return {};
            }
        }
        std::optional<V> ret;
        if (GET_LOCK(p).try_lock(tid, [=, &ret] {
                if (p->removed.load() || cptr->load() != c) return false;

                if (c->nt == LogLeaf) {
                    // The state might have changed if a key was inserted or
                    // deleted by logs (in the case of log leaves)

                    log_leaf *ll = (log_leaf *)c;
                    ret = ll->find(k, byte_pos);

                    if (!ret.has_value()) {
                        ART_GSTATS_ADD(tid, remove_codepath_rare_case_key_not_found,
                                   1);
                        return false;
                    }

                    assert(ll->kvsAreSorted());
                    assert(ll->noNullValues());

#ifndef NDEBUG
                    size_t oldSum = ll->getSumOfKeys();
                    auto keysBefore = ll->getKeys();
#endif

                    if (ll->is_appendable()) {
                        // assert(!ll->log.is_valid());
                        ART_GSTATS_ADD(tid, remove_codepath_append_log, 1);

#ifndef NDEBUG
                        auto sizeBefore = ll->size;
#endif
                        // std::cout << "Remove appending to log leaf" <<
                        // std::endl;
                        ll->append(tid, k, (V) nullptr, true);
                        assert(!ll->find(k, byte_pos).has_value());
                        assert(ll->kvsAreSorted());
                        assert(ll->noNullValues());

#ifndef NDEBUG
                        size_t newSum = ll->getSumOfKeys();
                        assert(oldSum - k == newSum);
                        auto keysAfter = ll->getKeys();
                        assert(keysAfter.size() == keysBefore.size() - 1);
                        assert(differenceIs(keysBefore, keysAfter, k, false));
#endif

#ifndef NDEBUG
                        assert(ll->size == sizeBefore);
#endif

                        return true;

                    } else {
                        assert(ll->log.is_valid());
                        assert(ll->secondLog.is_valid());
                        int realSize = ll->size;

                        if (ll->log.is_delete()) {
                            realSize--;
                        } else {
                            realSize++;
                        }

                        if (ll->secondLog.is_delete()) {
                            realSize--;
                        } else {
                            realSize++;
                        }

                        int newSize = realSize - 1;

                        if (newSize <= max_small_leaf_size) {
                            if (newSize == 0) {
                                ART_GSTATS_ADD(tid, remove_codepath_log2null, 1);
                                // std::cout << "Setting log leaf to nullptr" <<
                                // std::endl;

                                *cptr = nullptr;

                                log_leaf_pool.retire(tid, ll);

#ifndef NDEBUG
                                assert(oldSum - k == 0);
                                assert(keysBefore.size() == 1);
                                assert(keysBefore.at(0) == k);
#endif

                            } else {
                                ART_GSTATS_ADD(tid, remove_codepath_cow_b2s, 1);
                                // CoW log leaf into a new small leaf
                                // std::cout << "Remove CoW log leaf into new
                                // small leaf" << std::endl; std::cout << "New
                                // size: " << newSize << std::endl; std::cout <<
                                // "Going in the small leaf constructor" <<
                                // std::endl;
                                small_leaf *new_sl =
                                    small_leaf_pool.new_obj(tid, ll, k);
                                // std::cout << "----------------------------"
                                // << std::endl;
                                assert(new_sl->size == newSize);
                                assert(!new_sl->find(k, byte_pos).has_value());

#ifndef NDEBUG
                                size_t newSum = new_sl->getSumOfKeys();
                                assert(oldSum - k == newSum);
                                auto keysAfter = new_sl->getKeys();
                                assert(keysAfter.size() ==
                                       keysBefore.size() - 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    false));
#endif

                                new_sl->flush(tid);
                                *cptr = (node *)new_sl;
                                log_leaf_pool.retire(tid, ll);
                            }

                        } else {
                            // Now, this log leaf might hold more than
                            // max_big_leaf_size I may have 14 KV pairs in the
                            // array + 2 INSERT log entries So this remove will
                            // make newSize == 15 which cannot fit in a new,
                            // appendable log leaf I need to add split logic
                            // here

                            if (newSize < max_big_leaf_size) {
                                ART_GSTATS_ADD(tid, remove_codepath_cow_b2b, 1);
                                // std::cout << "Remove CoW log leaf into new
                                // log leaf" << std::endl; CoW log leaf into a
                                // new log leaf
                                assert(!ll->is_appendable());
                                assert(ll->log.is_valid());
                                assert(ll->secondLog.is_valid());
                                log_leaf *new_ll =
                                    log_leaf_pool.new_obj(tid, ll, k);
                                assert(new_ll->size == newSize);
                                assert(new_ll->size <= max_big_leaf_size);
                                assert(new_ll->is_appendable());
                                assert(new_ll->kvsAreSorted());
                                assert(new_ll->noNullValues());

                                assert(!new_ll->find(k, byte_pos).has_value());

#ifndef NDEBUG
                                size_t newSum = new_ll->getSumOfKeys();
                                assert(oldSum - k == newSum);
                                auto keysAfter = new_ll->getKeys();
                                assert(keysAfter.size() ==
                                       keysBefore.size() - 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    false));
#endif

                                new_ll->flush(tid);

                                *cptr = (node *)new_ll;
                                log_leaf_pool.retire(tid, ll);
                            } else {

#ifndef NDEBUG
                                size_t newSum = 0;
                                std::vector<K> keysAfter;
#endif

                                int n = newSize;

                                std::vector<KV> tmpVec;
                                tmpVec.reserve(n);
                                for (int i = 0; i < ll->size; i++) {
                                    tmpVec.push_back(ll->key_vals[i]);
                                }

                                if (ll->log.is_delete()) {
                                    tmpVec.erase(
                                        std::remove_if(
                                            tmpVec.begin(), tmpVec.end(),
                                            [&](const KV &kv) {
                                                return kv.key == ll->log.key;
                                            }),
                                        tmpVec.end());
                                } else {
                                    tmpVec.push_back(
                                        {ll->log.key, ll->log.value});
                                }

                                if (ll->secondLog.is_delete()) {
                                    tmpVec.erase(
                                        std::remove_if(
                                            tmpVec.begin(), tmpVec.end(),
                                            [&](const KV &kv) {
                                                return kv.key ==
                                                       ll->secondLog.key;
                                            }),
                                        tmpVec.end());
                                } else {
                                    tmpVec.push_back({ll->secondLog.key,
                                                      ll->secondLog.value});
                                }

#ifndef NDEBUG
                                int tmpVecSizeBefore = tmpVec.size();
#endif

                                // Also remove the key-value pair to be removed
                                tmpVec.erase(
                                    std::remove_if(tmpVec.begin(), tmpVec.end(),
                                                   [&](const KV &kv) {
                                                       return kv.key == k;
                                                   }),
                                    tmpVec.end());

#ifndef NDEBUG
                                assert(tmpVec.size() == tmpVecSizeBefore - 1);
#endif

                                assert(tmpVec.size() == newSize);
                                std::sort(tmpVec.begin(), tmpVec.end(),
                                          [](const KV &a, const KV &b) {
                                              return a.key < b.key;
                                          });
                                KV *tmp = tmpVec.data();
                                assert(tmpVec.size() == n);

#ifndef NDEBUG

                                for (int i = 1; i < n; i++) {
                                    assert(tmp[i - 1].key < tmp[i].key);
                                }

                                // Assert there's no duplicates
                                for (int i = 0; i < n; i++) {
                                    for (int j = i + 1; j < n; j++) {
                                        assert(tmp[i].key != tmp[j].key);
                                    }
                                }

#endif

                                auto gb = [&](const KV &kv) {
                                    return String::get_byte(kv.key, byte_pos);
                                };

                                int j = 0;
                                int start = 0;
                                int byteval = gb(tmp[0]);

                                node *children[n];

                                for (int i = 1; i < n; i++) {
                                    if (gb(tmp[i]) != byteval) {
                                        auto [new_l, is_log_leaf] =
                                            new_leaf(tid, byte_pos + 1,
                                                     &tmp[start], &tmp[i]);

                                        if (is_log_leaf) {
                                            assert(((log_leaf *)new_l)
                                                       ->kvsAreSorted());
                                            assert(((log_leaf *)new_l)
                                                       ->noNullValues());

                                            ((log_leaf *)new_l)->flush(tid);

#ifndef NDEBUG
                                            newSum += ((log_leaf *)new_l)
                                                          ->getSumOfKeys();

                                            auto newLeafKeys =
                                                ((log_leaf *)new_l)->getKeys();
                                            keysAfter.insert(
                                                keysAfter.end(),
                                                newLeafKeys.begin(),
                                                newLeafKeys.end());

#endif
                                            assert(!((log_leaf *)new_l)
                                                        ->find(k, byte_pos)
                                                        .has_value());
                                        } else {
                                            assert(((small_leaf *)new_l)
                                                       ->kvsAreSorted());
                                            assert(((small_leaf *)new_l)
                                                       ->noNullValues());

                                            ((small_leaf *)new_l)->flush(tid);

#ifndef NDEBUG
                                            newSum += ((small_leaf *)new_l)
                                                          ->getSumOfKeys();

                                            auto newLeafKeys =
                                                ((small_leaf *)new_l)
                                                    ->getKeys();
                                            keysAfter.insert(
                                                keysAfter.end(),
                                                newLeafKeys.begin(),
                                                newLeafKeys.end());

#endif
                                            assert(!((small_leaf *)new_l)
                                                        ->find(k, byte_pos)
                                                        .has_value());
                                        }

                                        children[j++] = (node *)new_l;
                                        start = i;
                                        byteval = gb(tmp[i]);
                                    }
                                }

                                auto [last_new_l, is_log_leaf] = new_leaf(
                                    tid, byte_pos + 1, &tmp[start], &tmp[n]);

                                if (is_log_leaf) {
                                    assert(((log_leaf *)last_new_l)
                                               ->kvsAreSorted());
                                    assert(((log_leaf *)last_new_l)
                                               ->noNullValues());

                                    ((log_leaf *)last_new_l)->flush(tid);

#ifndef NDEBUG
                                    newSum += ((log_leaf *)last_new_l)
                                                  ->getSumOfKeys();

                                    auto newLeafKeys =
                                        ((log_leaf *)last_new_l)->getKeys();
                                    keysAfter.insert(keysAfter.end(),
                                                     newLeafKeys.begin(),
                                                     newLeafKeys.end());

#endif
                                    assert(!((log_leaf *)last_new_l)
                                                ->find(k, byte_pos)
                                                .has_value());
                                } else {
                                    assert(((small_leaf *)last_new_l)
                                               ->kvsAreSorted());
                                    assert(((small_leaf *)last_new_l)
                                               ->noNullValues());

                                    ((small_leaf *)last_new_l)->flush(tid);

#ifndef NDEBUG
                                    newSum += ((small_leaf *)last_new_l)
                                                  ->getSumOfKeys();

                                    auto newLeafKeys =
                                        ((small_leaf *)last_new_l)->getKeys();
                                    keysAfter.insert(keysAfter.end(),
                                                     newLeafKeys.begin(),
                                                     newLeafKeys.end());
#endif
                                    assert(!((small_leaf *)last_new_l)
                                                ->find(k, byte_pos)
                                                .has_value());
                                }

                                children[j++] = (node *)last_new_l;

                                ART_GSTATS_ADD(tid, remove_codepath_cow_log2sparse,
                                           1);

                                sparse_node *new_s = sparse_pool.new_obj(
                                    tid, byte_pos, (node **)&children[0],
                                    (node **)&children[j]);

                                assert(new_s->size <= max_sparse_size);
                                assert(j == new_s->size);

                                new_s->flush(tid);

#ifndef NDEBUG
                                assert(oldSum - k == newSum);
                                assert(keysAfter.size() ==
                                       keysBefore.size() - 1);
                                assert(differenceIs(keysBefore, keysAfter, k,
                                                    false));
#endif

                                *cptr = (node *)new_s;

                                log_leaf_pool.retire(tid, ll);
                            }
                        }

                        return true;
                    }
                } else {
                    leaf *l = (leaf *)c;
                    ret = l->find(k, byte_pos);

#ifndef NDEBUG
                    size_t oldSum = l->getSumOfKeys();
                    auto keysBefore = l->getKeys();
#endif

                    if (l->size == 1) {
                        // std::cout << "Remove leaf with size 1" << std::endl;
                        /*
                        WRITE POINT
                        */
                        ART_GSTATS_ADD(tid, remove_codepath_set_null, 1);
                        *cptr = (node *)nullptr;

#ifndef NDEBUG
                        assert(oldSum - k == 0);
                        assert(keysBefore.size() == 1);
#endif

                        small_leaf_pool.retire(tid, (small_leaf *)l);
                    } else {
                        // CoW small leaf into a new small leaf
                        // std::cout << "Remove CoW small leaf into new small
                        // leaf" << std::endl;
                        ART_GSTATS_ADD(tid, remove_codepath_cow_s2s, 1);
                        assert(l->size > 1);
                        assert(l->size <= max_small_leaf_size);
                        small_leaf *new_sl = small_leaf_pool.new_obj(tid, l, k);
#ifndef NDEBUG
                        size_t newSum = new_sl->getSumOfKeys();
                        assert(oldSum - k == newSum);
                        auto keysAfter = new_sl->getKeys();
                        assert(keysAfter.size() == keysBefore.size() - 1);
                        assert(differenceIs(keysBefore, keysAfter, k, false));
#endif
                        assert(!new_sl->find(k, byte_pos).has_value());

                        new_sl->flush(tid);
                        *cptr = (node *)new_sl;
                        small_leaf_pool.retire(tid, (small_leaf *)l);
                    }
                    return true;
                }
                return true;
            }))
            return ret;
        return {};
    }

    std::optional<std::optional<V>> try_find(const K &k) {
        using ot = std::optional<std::optional<V>>;
        auto [gp, p, cptr, l, pos] = find_location(tid, root, k);
        if (GET_LOCK(p).read_lock(
                [&] { return (cptr->load() == l) && !p->removed.load(); }))
            if (l == nullptr)
                return ot(std::optional<V>());
            else
                return ot(((leaf *)l)->find(k, pos));
        else
            return ot();
    }

    std::optional<V> find_(const int tid, const K &k) {
        auto [gp, p, cptr, l, pos] = find_location(tid, root, k);
        auto ll = (leaf *)l;
        if (l == nullptr)
            return {};
        else {
            if (l->nt == Leaf) {
                return ((leaf *)l)->find(k, pos);
            } else {
                return ((log_leaf *)l)->find(k, pos);
            }
        }
    }

    std::optional<V> find(const int tid, const K &k) {
        return verlib::with_epoch(tid, [&] { return find_(tid, k); });
    }

    std::optional<V> find_locked(const K &k) {
        return flck::try_loop([&] { return try_find(k); });
    }

    template <typename AddF>
    static void range_internal(const int tid, node *a, AddF &add,
                               std::optional<K> start, std::optional<K> end,
                               int pos) {
        ART_GSTATS_ADD(tid, range_internal_codepath_root, 1);
        if (a == nullptr) {
            ART_GSTATS_ADD(tid, range_internal_codepath_isnull, 1);
            return;
        }
        std::optional<K> empty;

        if (a->nt == Leaf || a->nt == LogLeaf) {
            ART_GSTATS_ADD(tid, range_leaf_traversals, 1);

            if (a->nt == LogLeaf) {
#ifdef NoLogSpecificRangeLogic
                // Experimentally don't doing any range logic specific to log
                // leaves, to see how much overhead it adds to reconcile keys
                // and log entries
                log_leaf *ll = (log_leaf *)a;
                int s = 0;
                int e = a->size;
                if (start.has_value())
                    while (ll->key_vals[s].key < *start) s++;
                if (end.has_value())
                    while (ll->key_vals[e - 1].key > *end) e--;
                for (int i = s; i < e; i++)
                    add(ll->key_vals[i].key, ll->key_vals[i].value);
                return;
#else

                log_leaf *ll = (log_leaf *)a;
                int s = 0;
                int e = a->size;
                // First get the index of start and end keys that are relevant
                // to the range query
                if (start.has_value()) {
                    while (s < e && ll->key_vals[s].key < *start) s++;
                }
                if (end.has_value()) {
                    while (e > s && ll->key_vals[e - 1].key > *end) e--;
                }

                std::vector<KV> validKVs;
                validKVs.reserve(e - s + 2);  // +2 for possible log entries

                for (int i = s; i < e; i++) {
                    validKVs.push_back(ll->key_vals[i]);
                }

                auto localStamp = verlib::local_stamp;

                bool logValid, logVisible, secondLogValid, secondLogVisible;

                if (ll->log.is_valid()) {
                    logValid = true;
                    auto logStamp = ll->log.time_stamp.load();
                    assert(logStamp != verlib::tbd);

                    if (!verlib::global_stamp.less(localStamp, logStamp)) {
                        logVisible = true;
                        // The log entry is visible to this range query
                        if (!ll->log.is_delete()) {
                            // Check if the key being inserted is within range
                            if (((!start.has_value()) ||
                                 (ll->log.key >= *start)) &&
                                ((!end.has_value()) || (ll->log.key <= *end))) {
                                validKVs.push_back(
                                    {ll->log.key, ll->log.value});
                            }
                        } else {  // delete log entry
                            // Remove the key-value pair from validKVs if it
                            // exists
                            validKVs.erase(
                                std::remove_if(validKVs.begin(), validKVs.end(),
                                               [&](const KV &kv) {
                                                   return kv.key == ll->log.key;
                                               }),
                                validKVs.end());
                        }

                        // Second log can be visible only if the first log is
                        // visible.
                        if (ll->secondLog.is_valid()) {
                            secondLogValid = true;
                            auto secondLogStamp =
                                ll->secondLog.time_stamp.load();
                            assert(secondLogStamp != verlib::tbd);

                            if (!verlib::global_stamp.less(localStamp,
                                                           secondLogStamp)) {
                                secondLogVisible = true;
                                // The log entry is visible to this range query
                                if (!ll->secondLog.is_delete()) {
                                    // Check if the key being inserted is within
                                    // range
                                    if (((!start.has_value()) ||
                                         (ll->secondLog.key >= *start)) &&
                                        ((!end.has_value()) ||
                                         (ll->secondLog.key <= *end))) {
                                        validKVs.push_back(
                                            {ll->secondLog.key,
                                             ll->secondLog.value});
                                    }
                                } else {  // delete log entry
                                    // Remove the key-value pair from validKVs
                                    // if it exists
                                    validKVs.erase(
                                        std::remove_if(
                                            validKVs.begin(), validKVs.end(),
                                            [&](const KV &kv) {
                                                return kv.key ==
                                                       ll->secondLog.key;
                                            }),
                                        validKVs.end());
                                }
                            }
                        }
                    }
                }

                // Now add all validKVs to the result. No need to sort:
                for (const auto &kv : validKVs) { add(kv.key, kv.value); }

                // now based on the booleans fill out the appropriate gstast
                if (logValid && logVisible) {
                    if (secondLogValid && secondLogVisible) {
                        ART_GSTATS_ADD(tid, range_internal_codepath_log_both_logs,
                                   1);
                    } else {
                        ART_GSTATS_ADD(
                            tid, range_internal_codepath_log_first_log_only, 1);
                    }
                } else {
                    ART_GSTATS_ADD(tid, range_internal_codepath_log_nologs, 1);
                }

                return;
#endif
            } else {
                ART_GSTATS_ADD(tid, range_internal_codepath_sleaf, 1);
                leaf *l = (leaf *)a;
                int s = 0;
                int e = a->size;
                if (start.has_value())
                    while (l->key_vals[s].key < *start) s++;
                if (end.has_value())
                    while (l->key_vals[e - 1].key > *end) e--;
                for (int i = s; i < e; i++)
                    add(l->key_vals[i].key, l->key_vals[i].value);
                return;
            }
        }
        for (int i = pos; i < a->byte_num; i++) {
            if (start == empty && end == empty) break;
            if (start.has_value() && String::get_byte(start.value(), i) >
                                         String::get_byte(a->key, i) ||
                end.has_value() && String::get_byte(end.value(), i) <
                                       String::get_byte(a->key, i)) {
                ART_GSTATS_ADD(tid, range_internal_codepath_no_overlap, 1);
                return;
            }
            if (start.has_value() && String::get_byte(start.value(), i) <
                                         String::get_byte(a->key, i))
                start = empty;
            if (end.has_value() && String::get_byte(end.value(), i) >
                                       String::get_byte(a->key, i)) {
                end = empty;
            }
        }
        int sb = start.has_value()
                     ? String::get_byte(start.value(), a->byte_num)
                     : 0;
        int eb =
            end.has_value() ? String::get_byte(end.value(), a->byte_num) : 255;
        if (a->nt == Full) {
            ART_GSTATS_ADD(tid, range_internal_codepath_full, 1);
            for (int i = sb; i <= eb; i++)
                range_internal(tid,
                               ((full_node *)a)->children[i].read_snapshot(),
                               add, start, end, a->byte_num);
        } else if (a->nt == Indirect) {
            ART_GSTATS_ADD(tid, range_internal_codepath_indirect, 1);
            for (int i = sb; i <= eb; i++) {
                indirect_node *ai = (indirect_node *)a;
#ifdef OrderedIndirectNodes
                auto [which_word, which_bit] = ai->word_idx_and_bit(i);
                auto mask = 1ULL << which_bit;
                if (ai->bitmap[which_word] & mask) {
                    auto which_ptr = ai->ones_to_left(which_word, which_bit);
                    range_internal(tid, ai->ptr[which_ptr].read_snapshot(), add,
                                   start, end, a->byte_num);
                }
#else
                int o = ai->idx[i];
                if (o != -1) {
                    range_internal(tid, ai->ptr[o].read_snapshot(), add, start,
                                   end, a->byte_num);
                }
#endif
            }
        } else {  // Sparse
            ART_GSTATS_ADD(tid, range_internal_codepath_sparse, 1);
            sparse_node *as = (sparse_node *)a;
            for (int i = 0; i < as->size; i++) {
                int b = as->keys[i];
                if (b >= sb && b <= eb)
                    range_internal(tid, as->ptr[i].read_snapshot(), add, start,
                                   end, a->byte_num);
            }
        }
    }

    template <typename AddF>
    void range_(const int tid, AddF &add, const K &start, const K &end) {
        range_internal(tid, root, add, std::optional<K>(start),
                       std::optional<K>(end), 0);
    }

    void rangeDriver(const int tid, const K &lo, const K &hi,
                     K *const resultKeys, V *const resultValues,
                     int &resultSize) {
        using kvpair = std::pair<K, V>;

#ifndef AnalyticalRangeQuery
        std::vector<kvpair> result;
        auto add = [&](K k, V v) { result.push_back(std::make_pair(k, v)); };
#else
        size_t sum = 0;
        auto add = [&](K k, V v) { sum += k; };
#endif
        verlib::with_snapshot(tid, [&] { range_(tid, add, lo, hi); });

#ifndef AnalyticalRangeQuery
        resultSize = result.size();
#else
        resultSize = sum;
#endif
        // resultKeys = new K[resultSize];
        // resultValues = new V[resultSize];
        // for (int i = 0; i < resultSize; i++) {
        //     resultKeys[i] = result[i].first;
        //     resultValues[i] = result[i].second;
        // }
    }

    ordered_map() {
        std::cout << "In constructor of ds: " << parlay::num_workers()
                  << std::endl;
        auto r = full_pool.new_obj(0);
        r->byte_num = 0;
        root = (node *)r;
        std::cout << "End of constructor of ds: " << parlay::num_workers()
                  << std::endl;
    }

    ordered_map(const int numThreads, const K &KEY_ANY, const K &KEY_MAX,
                const V &NO_VALUE)
        : NO_VALUE(NO_VALUE) {
        std::cout << "In constructor of ds: " << parlay::num_workers()
                  << std::endl;
        auto r = full_pool.new_obj(0);
        r->byte_num = 0;
        root = (node *)r;
        std::cout << "End of constructor of ds: " << parlay::num_workers()
                  << std::endl;

        std::cout << "Size of verlib::versioned: " << sizeof(verlib::versioned)
                  << std::endl;
        std::cout << "Size of header: " << sizeof(header) << std::endl;
        std::cout << "Size of node(general): " << sizeof(node) << std::endl;

        std::cout << "FULL NODE: " << std::endl;

        std::cout << "Size of full_node: " << sizeof(full_node) << std::endl;
        std::cout << "Offset of timestamp: " << offsetof(full_node, time_stamp)
                  << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(full_node, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(full_node, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(full_node, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(full_node, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(full_node, key) << std::endl;
        std::cout << "Offset of removed: " << offsetof(full_node, removed)
                  << std::endl;
#ifndef RemoveLockFieldFromNodes
        std::cout << "Offset of lock: " << offsetof(full_node, lck)
                  << std::endl;
#endif
        std::cout << "Offset of children: " << offsetof(full_node, children)
                  << std::endl;
        std::cout << std::endl;

        std::cout << "INDIRECT NODE: " << std::endl;
        std::cout << "Size of indirect_node: " << sizeof(indirect_node)
                  << std::endl;
        std::cout << "Offset of time_stamp: "
                  << offsetof(indirect_node, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(indirect_node, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(indirect_node, nt)
                  << std::endl;
        std::cout << "Offset of size: " << offsetof(indirect_node, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(indirect_node, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(indirect_node, key)
                  << std::endl;
        std::cout << "Offset of removed: " << offsetof(indirect_node, removed)
                  << std::endl;
#ifndef RemoveLockFieldFromNodes
        std::cout << "Offset of lock: " << offsetof(indirect_node, lck)
                  << std::endl;
#endif
#ifdef OrderedIndirectNodes
        std::cout << "Offset of bitmap: " << offsetof(indirect_node, bitmap)
                  << std::endl;
        std::cout << "Offset of ptr: " << offsetof(indirect_node, ptr)
                  << std::endl;
#else
        std::cout << "Offset of idx: " << offsetof(indirect_node, idx)
                  << std::endl;
        std::cout << "Offset of ptr: " << offsetof(indirect_node, ptr)
                  << std::endl;
#endif
        std::cout << std::endl;

        std::cout << "SPARSE NODE: " << std::endl;
        std::cout << "Size of sparse_node: " << sizeof(sparse_node)
                  << std::endl;
        std::cout << "Offset of time_stamp: "
                  << offsetof(sparse_node, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(sparse_node, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(sparse_node, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(sparse_node, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(sparse_node, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(sparse_node, key)
                  << std::endl;
        std::cout << "Offset of removed: " << offsetof(sparse_node, removed)
                  << std::endl;
#ifndef RemoveLockFieldFromNodes
        std::cout << "Offset of lock: " << offsetof(sparse_node, lck)
                  << std::endl;
#endif
        std::cout << "Offset of keys: " << offsetof(sparse_node, keys)
                  << std::endl;
        std::cout << "Offset of ptr: " << offsetof(sparse_node, ptr)
                  << std::endl;
        std::cout << std::endl;

        std::cout << "SMALL LEAF NODE: " << std::endl;
        std::cout << "Size of small_leaf: " << sizeof(small_leaf) << std::endl;
        std::cout << "Offset of time_stamp: "
                  << offsetof(small_leaf, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(small_leaf, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(small_leaf, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(small_leaf, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(small_leaf, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(small_leaf, key)
                  << std::endl;
        std::cout << "Offset of key_vals: " << offsetof(small_leaf, key_vals)
                  << std::endl;
        std::cout << std::endl;

        std::cout << "BIG LEAF NODE: " << std::endl;
        std::cout << "Size of big_leaf: " << sizeof(big_leaf) << std::endl;
        std::cout << "Offset of time_stamp: " << offsetof(big_leaf, time_stamp)
                  << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(big_leaf, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(big_leaf, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(big_leaf, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(big_leaf, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(big_leaf, key) << std::endl;
        std::cout << "Offset of key_vals: " << offsetof(big_leaf, key_vals)
                  << std::endl;
        std::cout << std::endl;

        std::cout << "LOG LEAF NODE: " << std::endl;
        std::cout << "Size of log_leaf: " << sizeof(log_leaf) << std::endl;
        std::cout << "Offset of time_stamp: " << offsetof(log_leaf, time_stamp)
                  << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(log_leaf, next_version) << std::endl;
        std::cout << "Offset of nt: " << offsetof(log_leaf, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(log_leaf, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(log_leaf, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(log_leaf, key) << std::endl;

        std::cout << "Offset of first log: " << offsetof(log_leaf, log)
                  << std::endl;
        std::cout << "Offset of second log: " << offsetof(log_leaf, secondLog)
                  << std::endl;

        std::cout << "Offset of key_vals: " << offsetof(log_leaf, key_vals)
                  << std::endl;
        std::cout << std::endl;
    }

    void print() {
        std::function<void(node *)> prec;
        prec = [&](node *p) {
            if (p == nullptr) return;
            switch (p->nt) {
                case Leaf: {
                    auto l = (leaf *)p;
                    std::cout << std::hex << l->key << ":" << l->key_vals[0].key
                              << std::dec << ", ";
                    return;
                }
                case Full: {
                    auto f_n = (full_node *)p;
                    for (int i = 0; i < 256; i++) {
                        prec(f_n->children[i].load());
                    }
                    return;
                }
                case Indirect: {
                    auto i_n = (indirect_node *)p;
                    for (int i = 0; i < 256; i++) {
                        int j = i_n->idx[i];
                        if (j != -1) prec(i_n->ptr[j].load());
                    }
                    return;
                }
                case Sparse: {
                    using pr = std::pair<int, node *>;
                    auto s_n = (sparse_node *)p;
                    std::vector<pr> v;
                    for (int i = 0; i < s_n->size; i++)
                        v.push_back(
                            std::make_pair(s_n->keys[i], s_n->ptr[i].load()));
                    std::sort(v.begin(), v.end());
                    for (auto x : v) prec(x.second);
                    return;
                }
            }
        };
        prec(root);
        std::cout << std::endl;
    }

    static void retire_recursive(node *p) {
        // if (p == nullptr) return;
        // if (p->nt == Leaf) {
        //     if (p->size > max_small_leaf_size)
        //         big_leaf_pool.retire((big_leaf *) p);
        //     else small_leaf_pool.retire((small_leaf *) p);
        // } else if (p->nt == Sparse) {
        //     auto pp = (sparse_node *) p;
        //     parlay::parallel_for(0, pp->size, [&](size_t i) {
        //         retire_recursive(pp->ptr[i].load());
        //     });
        //     sparse_pool.retire(pp);
        // } else if (p->nt == Indirect) {
        //     auto pp = (indirect_node *) p;
        //     parlay::parallel_for(0, pp->size, [&](size_t i) {
        //         retire_recursive(pp->ptr[i].load());
        //     });
        //     indirect_pool.retire(pp);
        // } else {
        //     auto pp = (full_node *) p;
        //     parlay::parallel_for(0, 256, [&](size_t i) {
        //         retire_recursive(pp->children[i].load());
        //     });
        //     full_pool.retire(pp);
        // }
    }

    ~ordered_map() {
        //        retire_recursive(root);
    }

    long check() {
        std::function<size_t(node *)> crec;
        crec = [&](node *p) -> size_t {
            if (p == nullptr) return 0;
            switch (p->nt) {
                case Leaf: {
                    leaf *l = (leaf *)p;
                    for (int i = 1; i < l->size; i++)
                        if (!(l->key_vals[i - 1].key < l->key_vals[i].key)) {
                            std::cout << "out of order at leaf: " << i << ", "
                                      << (int)l->size << ", " << std::hex
                                      << l->key_vals[i - 1].key << ", "
                                      << l->key_vals[i].key << std::dec
                                      << std::endl;
                            abort();
                        }
                    return l->size;
                }
                case Full: {
                    auto f_n = (full_node *)p;
                    auto x = parlay::tabulate(256, [&](size_t i) {
                        return crec(f_n->children[i].load());
                    });
                    return parlay::reduce(x);
                }
                case Indirect: {
                    auto i_n = (indirect_node *)p;
                    auto x = parlay::tabulate(256, [&](size_t i) {
                        int j = i_n->idx[i];
                        return (j == -1) ? 0 : crec(i_n->ptr[j].load());
                    });
                    return parlay::reduce(x);
                }
                case Sparse: {
                    auto s_n = (sparse_node *)p;
                    auto x = parlay::tabulate(s_n->size, [&](size_t i) {
                        return crec(s_n->ptr[i].load());
                    });
                    return parlay::reduce(x);
                }
            }
            return 0;
        };
        size_t cnt = crec(root);
        return cnt;
    }

    static void clear() {
        full_pool.clear();
        indirect_pool.clear();
        sparse_pool.clear();
        small_leaf_pool.clear();
        big_leaf_pool.clear();
        log_leaf_pool.clear();
    }

    static void reserve(size_t n) {}

    static void shuffle(size_t n) {
        full_pool.shuffle(n / 100);
        indirect_pool.shuffle(n / 10);
        sparse_pool.shuffle(n / 5);
        small_leaf_pool.shuffle(n);
        big_leaf_pool.shuffle(n);
        log_leaf_pool.shuffle(n);
    }

    static void stats() {
        full_pool.stats();
        indirect_pool.stats();
        sparse_pool.stats();
        small_leaf_pool.stats();
        big_leaf_pool.stats();
        log_leaf_pool.stats();
    }

#ifdef ReturnBool
    bool insert_(const int tid, const K &k, const V &v) {
        return flck::try_loop([&]() { return try_insert(tid, k, v); });
    }

    bool insert(const int tid, const K &k, const V &v) {
        return verlib::with_epoch(tid, [=] { return insert_(tid, k, v); });
    }

    std::optional<bool> try_insert(const int tid, const K &k, const V &v,
                                   bool upsert = false) {
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        // std::cout << byte_pos << std::endl;
        if (c != nullptr && c->is_leaf() &&
            ((leaf *)c)->find(k, byte_pos).has_value())  // already in the tree
            if (GET_LOCK(p).read_lock(
                    [&] { return (cptr->load() == c) && !p->removed.load(); }))
                return false;
            else
                return {};

        if (cptr !=
            nullptr) {  // child pointer exists, always true for full node
            if (GET_LOCK(p).try_lock(tid, [=] {
                    // exit and retry if state has changed
                    if (p->removed.load() || cptr->load() != c) return false;

                    // fill a null pointer with the new leaf
                    if (c == nullptr)
                        (*cptr) = (node *)small_leaf_pool.new_obj(tid, k, v);
                    else if (c->is_leaf()) {
                        leaf *l = (leaf *)c;
                        small_leaf *sl = (small_leaf *)c;
                        big_leaf *bl = (big_leaf *)c;
                        if (l->size < max_small_leaf_size) {
                            *cptr = (node *)small_leaf_pool.new_obj(
                                tid, byte_pos, l, k, v);
                            small_leaf_pool.retire(tid, sl);
                        } else if (l->size == max_small_leaf_size) {
                            *cptr = (node *)big_leaf_pool.new_obj(tid, byte_pos,
                                                                  l, k, v);
                            small_leaf_pool.retire(tid, sl);
                        } else if (l->size < max_big_leaf_size) {
                            *cptr = (node *)big_leaf_pool.new_obj(tid, byte_pos,
                                                                  l, k, v);
                            big_leaf_pool.retire(tid, bl);
                        } else {  // too large
                            int n = max_big_leaf_size + 1;

                            // insert new key-value pair into l and put result
                            // into tmp
                            KV tmp[n];
                            leaf::insert(l->key_vals, tmp, n - 1, k, v);
                            // for (int i=0; i < max_big_leaf_size; i++)
                            //          tmp[i] = l->key_vals[i];
                            // tmp[max_big_leaf_size] = KV{k,v};
                            // std::sort(tmp, tmp + n, [&] (KV& a, KV& b)
                            // {return a.key < b.key;});

                            // break tmp up into multiple leafs base on byte
                            // value at byte_pos
                            auto gb = [&](const KV &kv) {
                                return String::get_byte(kv.key, byte_pos);
                            };
                            int j = 0;
                            int start = 0;
                            int byteval = gb(tmp[0]);
                            node *children[n];
                            for (int i = 1; i < n; i++) {
                                if (gb(tmp[i]) != byteval) {
                                    children[j++] =
                                        new_leaf(tid, byte_pos + 1, &tmp[start],
                                                 &tmp[i]);
                                    start = i;
                                    byteval = gb(tmp[i]);
                                }
                            }
                            children[j++] = new_leaf(tid, byte_pos + 1,
                                                     &tmp[start], &tmp[n]);

                            // insert the new leaves into a sparse node
                            *cptr = (node *)sparse_pool.new_obj(
                                tid, byte_pos, &children[0], &children[j]);
                            big_leaf_pool.retire(tid, bl);
                        }
                    } else {  // not a leaf
                        node *new_l =
                            (node *)small_leaf_pool.new_obj(tid, k, v);
                        *cptr = (node *)sparse_pool.new_obj(tid, byte_pos, c,
                                                            c->key, new_l, k);
                    }
                    return true;
                }))
                return true;
        } else {  // no child pointer, need to add
            bool x = add_child(tid, gp, p, k, v);
            if (x) return true;
        }
        return {};
    }

#endif

#ifdef ReturnBool

    bool remove_(const int tid, const K &k) {
        return flck::try_loop([&]() { return try_remove(tid, k); });
    }

    bool remove(const int tid, const K &k) {
        return verlib::with_epoch(tid, [=] { return remove_(tid, k); });
    }

    // currently a "lazy" remove that only removes
    //   1) the leaf
    //   2) its parent if it is sparse with just two children
    std::optional<bool> try_remove(const int tid, const K &k) {
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        // if not found return
        if (c == nullptr) return false;  // TODO: is this correct?
        if (c == nullptr ||
            !(c->is_leaf() && ((leaf *)c)->find(k, byte_pos).has_value()))
            if (GET_LOCK(p).read_lock(
                    [&] { return (cptr->load() == c) && !p->removed.load(); }))
                return false;
            else
                return {};
        if (GET_LOCK(p).try_lock(tid, [=] {
                if (p->removed.load() || cptr->load() != c) return false;
                leaf *l = (leaf *)c;
                if (l->size == 1) {
                    node *other_child = single_other_child(p, c);
                    // if (other_child != nullptr && gp->nt != Sparse) {
                    //   // if parent will become singleton try to remove parent
                    //   as well return GET_LOCK(gp).try_lock([=] {
                    //     auto child_ptr = get_child(gp, p->key);
                    //     if (gp->removed.load() || child_ptr->load() != p)
                    // return false;
                    //     *child_ptr = other_child;
                    //     p->removed = true;
                    //     sparse_pool.retire((sparse_node*) p);
                    //     small_leaf_pool.retire((small_leaf*) l);
                    //     return true;});
                    // } else
                    {  // just remove child
                        *cptr = nullptr;
                        small_leaf_pool.retire(tid, (small_leaf *)l);
                        return true;
                    }
                } else {  // at least 2 in leaf
                    if (l->size > max_small_leaf_size + 1) {
                        *cptr = (node *)big_leaf_pool.new_obj(tid, l, k);
                        big_leaf_pool.retire(tid, (big_leaf *)l);
                    } else if (l->size == max_small_leaf_size + 1) {
                        *cptr = (node *)small_leaf_pool.new_obj(tid, l, k);
                        big_leaf_pool.retire(tid, (big_leaf *)l);
                    } else {
                        *cptr = (node *)small_leaf_pool.new_obj(tid, l, k);
                        small_leaf_pool.retire(tid, (small_leaf *)l);
                    }
                    return true;
                }
            }))
            return true;
        return {};
    }

#endif
};

template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::full_node>
    ordered_map<K, V, S>::full_pool;
template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::indirect_node>
    ordered_map<K, V, S>::indirect_pool;
template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::sparse_node>
    ordered_map<K, V, S>::sparse_pool;
template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::small_leaf>
    ordered_map<K, V, S>::small_leaf_pool;
template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::big_leaf>
    ordered_map<K, V, S>::big_leaf_pool;
template <typename K, typename V, typename S>
verlib::memory_pool<typename ordered_map<K, V, S>::log_leaf>
    ordered_map<K, V, S>::log_leaf_pool;