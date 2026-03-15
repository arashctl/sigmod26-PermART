#include <artifact_parlay/primitives.h>
#include <artifact_verlib/verlib.h>

#include <bitset>
#include <iostream>

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
#define MAX_SPARSE_SIZE 16
#endif

#ifndef MAX_SMALL_LEAF_SIZE
#define MAX_SMALL_LEAF_SIZE 2
#endif

#ifndef MAX_MEDIUM_LEAF_SIZE
#define MAX_MEDIUM_LEAF_SIZE 6
#endif

#ifndef MAX_BIG_LEAF_SIZE
#define MAX_BIG_LEAF_SIZE 14
#endif


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
    static constexpr int max_medium_leaf_size = MAX_MEDIUM_LEAF_SIZE;
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

    struct alignas(ALIGNMENT_SIZE) full_node : node {
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
#ifdef DO_FLUSH_NODES
            int flushCount =
                durableTools::FLUSH_LINES(tid, this, sizeof(full_node));
#endif
        }

        full_node() : node(Full, 0) {}
    };
    // Up to max_indirect_size entries, with array of 256 1-byte
    // pointers to the entries.  Adding a new child requires a copy.
    // Updating an existing child is done in place.

    struct alignas(ALIGNMENT_SIZE) indirect_node : node {
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
#ifdef DO_FLUSH_NODES
            int flushCount =
                durableTools::FLUSH_LINES(tid, this, sizeof(indirect_node));
#endif
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
#ifdef DO_FLUSH_NODES
            int flushCount =
                durableTools::FLUSH_LINES(tid, this, sizeof(indirect_node));
#endif
        }

        // an empty indirect node
        indirect_node() : node(Indirect, 0){};
#endif
    };

    // Up to max_sparse_size entries each consisting of a key and
    // pointer.  The keys are immutable, but the pointers can be
    // changed.  i.e. Adding a new child requires copying, but updating
    // a child can be done in place.
    struct alignas(ALIGNMENT_SIZE) sparse_node : node {
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

        inline void flush(const int tid) {
#ifdef DO_FLUSH_NODES
            int flushCount =
                durableTools::FLUSH_LINES(tid, this, sizeof(sparse_node));
#endif
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

        // an empty sparse node
        sparse_node() : node(Sparse, 0) {}
    };

    struct KV {
        K key;
        V value;
    };


    template <int MaxSize>
    struct alignas(ALIGNMENT_SIZE) generic_leaf : header {
        using leaf_ptr = generic_leaf<0> *;

        KV key_vals[MaxSize];

        void print() {
#ifndef NDEBUG
            std::cout << "Leaf: " << std::endl;
            for (int i = 0; i < header::size; i++) {
                std::cout << key_vals[i].key << " ";
            }
            std::cout << std::endl;

            std::cout << "-------------------------------------\n";
#endif
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

        std::optional<V> find(const int tid, const K &k, int byte_pos) {
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
#ifdef DO_FLUSH_NODES
            int flushCount = durableTools::FLUSH_LINES(
                tid, this, sizeof(generic_leaf<MaxSize>));
#endif
        }

        // create singleton leaf
        generic_leaf(K &key, V &value)
            : header(key, Leaf, 1, String::length(key)) {
            key_vals[0] = KV{key, value};

            // No need to bloom filter here as this is a small leaf
        }

        // create multi leaf
        generic_leaf(int byte_pos, KV *start, KV *end)
            : header(start->key, Leaf, end - start,
                     first_diff(byte_pos, start, end)) {
            for (int i = 0; i < (end - start); i++) { key_vals[i] = start[i]; }
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

    };

    using leaf = generic_leaf<0>;  // only used generically
    using small_leaf = generic_leaf<max_small_leaf_size>;
    using medium_leaf = generic_leaf<max_medium_leaf_size>;
    using big_leaf = generic_leaf<max_big_leaf_size>;

    static verlib::memory_pool<full_node> full_pool;
    static verlib::memory_pool<indirect_node> indirect_pool;
    static verlib::memory_pool<sparse_node> sparse_pool;
    static verlib::memory_pool<small_leaf> small_leaf_pool;
    static verlib::memory_pool<medium_leaf> medium_leaf_pool;
    static verlib::memory_pool<big_leaf> big_leaf_pool;

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
        GSTATS_ADD(tid, add_child_calls, 1);
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
                        GSTATS_ADD(tid, indirect_to_full, 1);

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
                        GSTATS_ADD(tid, indirect_to_indirect, 1);

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
                        GSTATS_ADD(tid, sparse_to_indirect, 1);

                        /*
                        NODE CREATION POINT:
                        A new indirect node is created, holding:
                        Whatever was in the sparse node, plus the new leaf
                        containing the key-value pair to be inserted
                        */

                        indirect_node *new_i = indirect_pool.new_init(
                            tid, [=](indirect_node *i_n) {

#ifdef OrderedIndirectNodes
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
                        GSTATS_ADD(tid, sparse_to_sparse, 1);

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


    static leaf *new_leaf(const int tid, int byte_pos, KV *start, KV *end) {

        if ((end - start) > max_medium_leaf_size) {
            return (leaf *)big_leaf_pool.new_obj(tid, byte_pos, start, end);
        }
        if ((end - start) > max_small_leaf_size) {
            return (leaf *)medium_leaf_pool.new_obj(tid, byte_pos, start, end);
        }
        return (leaf *)small_leaf_pool.new_obj(tid, byte_pos, start, end);
    }

    V insert_(const int tid, const K &k, const V &v) {
        // int attempts = 0;
        V ret = flck::try_loop([&]() {
            // attempts++;
            return try_insert(tid, k, v);
        });
        // if (attempts > 100) attempts = 100;
        // GSTATS_ADD_IX(tid, insert_attempts_histogram, 1, attempts);
        // if (ret == v) {
        //     assert(find(tid, k).has_value());
        // }
        return ret;
    }

    V insert(const int tid, const K &k, const V &v) {
        return verlib::with_epoch(tid, [=] { return insert_(tid, k, v); });
    }


    std::optional<V> try_insert(const int tid, const K &k, const V &v,
                                bool upsert = false) {
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        // std::cout << byte_pos << std::endl;
        if (c != nullptr && c->is_leaf() &&
            ((leaf *)c)
                ->find(tid, k, byte_pos)
                .has_value()) {  // already in the tree
            if (GET_LOCK(p).read_lock([&] {
                    return (cptr->load() == c) && !p->removed.load();
                })) {
                auto ret = ((leaf *)c)->find(tid, k, byte_pos);
                return ret;
            } else {
                return {};
            }
        }

        if (cptr !=
            nullptr) {  // child pointer exists, always true for full node
            if (GET_LOCK(p).try_lock(tid, [=] {
                    // exit and retry if state has changed
                    if (p->removed.load() || cptr->load() != c) return false;

                    // fill a null pointer with the new leaf
                    if (c == nullptr) {
                        GSTATS_ADD(tid, new_sleaf, 1);
                        /*
                        NODE CREATION POINT:
                           A new small leaf is created, holding:
                             Only the new key-value pair to be inserted
                        */
                        small_leaf *new_l = small_leaf_pool.new_obj(tid, k, v);
                        /*‌
                        NEW NODE FLUSH POINT
                        */
                        new_l->flush(tid);
                        /*
                        WRITE POINT:
                        The child pointer is updated to point to the new small
                        leaf.
                        */
                        *cptr = (node *)new_l;

                    } else if (c->is_leaf()) {
                        // if c is a leaf, there are a few possibilities
                        // 1) c is a small leaf, and we can add to it (easy)
                        // 2) c is a small leaf, and we need to replace it with
                        // a big leaf (easy) 3) c is a big leaf, and we can add
                        // to it (easy) 4) c is a big leaf, and we need to
                        // replace it ith a sparse node (has a twist on how to
                        // add leaves to the resulting sparse node)

                        leaf *l = (leaf *)c;
                        small_leaf *sl = (small_leaf *)c;
                        big_leaf *bl = (big_leaf *)c;

                        if (l->size < max_small_leaf_size) {
                            GSTATS_ADD(tid, sleaf_to_sleaf, 1);

                            /*
                            NODE CREATION POINT:
                            A new small leaf is created, holding:
                            1. The old leaf's key-value pair(s) (in this case
                            only one because max size of small leaf is 2)
                            2. The new key-value pair to be inserted
                            */
                            small_leaf *new_sl =
                                small_leaf_pool.new_obj(tid, byte_pos, l, k, v);

                            /*
                            NEW NODE FLUSH POINT
                            */
                            new_sl->flush(tid);
                            /*
                            WRITE POINT:
                            The child pointer is updated to point to the new
                            small leaf.
                            */
                            *cptr =
                                (node *)new_sl;  // colocate the flush-bit with
                                                 // the keyloc you're writing.

                            small_leaf_pool.retire(tid, sl);
                        } else if (l->size == max_small_leaf_size) {
                            GSTATS_ADD(tid, sleaf_to_bleaf, 1);
                            /*
                            NODE CREATION POINT:
                            A new big leaf is created, holding:
                            1. The old small leaf's key-value pair(s) (in this
                            case two pairs)
                            2. The new key-value pair to be inserted
                            */
                            big_leaf *new_bl =
                                big_leaf_pool.new_obj(tid, byte_pos, l, k, v);

                            /*
                            NEW NODE FLUSH POINT
                            */
                            new_bl->flush(tid);
                            /*
                            WRITE POINT:
                            The child pointer is updated to point to the new big
                            leaf.
                            */
                            *cptr = (node *)new_bl;

                            small_leaf_pool.retire(tid, sl);
                        }

                        else if (l->size < max_big_leaf_size) {
                            GSTATS_ADD(tid, bleaf_to_bleaf, 1);

                            /*
                            NODE CREATION POINT:
                            A new big leaf is created, holding:
                            1. The old big leaf's key-value pair(s)
                            2. The new key-value pair to be inserted
                            */
                            big_leaf *new_bl =
                                big_leaf_pool.new_obj(tid, byte_pos, l, k, v);

                            /*
                            NEW NODE FLUSH POINT
                            */
                            new_bl->flush(tid);

                            /*
                            WRITE POINT:
                            The child pointer is updated to point to the new big
                            leaf.
                            */
                            *cptr = (node *)new_bl;

                            big_leaf_pool.retire(tid, bl);
                        } else {  // too large, replaced with a sparse node
                            GSTATS_ADD(tid, bleaf_to_sparse, 1);
                            int n = max_big_leaf_size + 1;

                            // insert new key-value pair into l and put result
                            // into tmp
                            KV tmp[n];
                            leaf::insert(
                                l->key_vals, tmp, n - 1, k,
                                v);  // final size: n = (n-1 from leaf where
                                     // n-1==max_big_leaf_size) + (1 from newly
                                     // to-be-inserted key-value pair)
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
                            leaf *children[n];  // IMPORTANT: node *children[n]
                                                // is an array of pointers to
                                                // nodes, which are the children
                                                // of the new sparse node.
                                                // Children is a stack array.
                            for (int i = 1; i < n; i++) {
                                if (gb(tmp[i]) != byteval) {
                                    /*
                                    NODE CREATION POINT:
                                    A new leaf is created, holding key-value
                                    pairs from tmp[start] to tmp[i-1]
                                    (inclusive) The new leaf can be a small leaf
                                    or a big leaf, depending on the number of
                                    key-value pairs
                                    */

                                    leaf *new_l =
                                        new_leaf(tid, byte_pos + 1, &tmp[start],
                                                 &tmp[i]);
                                    /*‌
                                    NEW NODE FLUSH POINT
                                    */
                                    // TODO: make sure sizeof is returning the
                                    // correct size here, because the leaf can
                                    // be big or small quick possible fix: based
                                    // on the difference between i and start
                                    // (i-start), decide if the leaf is big or
                                    // small

                                    i - start > max_small_leaf_size
                                        ? ((big_leaf *)new_l)->flush(tid)
                                        : ((small_leaf *)new_l)->flush(tid);

                                    children[j++] = new_l;

                                    start = i;
                                    byteval = gb(tmp[i]);
                                }
                            }

                            /*
                            NODE CREATION POINT:
                            A new leaf is created, holding key-value pairs from
                            tmp[start] to tmp[n-1] (inclusive)
                            */
                            leaf *last_new_l = new_leaf(tid, byte_pos + 1,
                                                        &tmp[start], &tmp[n]);

                            /*
                            NEW NODE FLUSH POINT
                            */
                            // TODO: make sure sizeof is returning the correct
                            // size here, because the leaf can be big or small
                            // quick possible fix: based on the difference
                            // between i and start (i-start), decide if the leaf
                            // is big or small
                            n - start > max_small_leaf_size
                                ? ((big_leaf *)last_new_l)->flush(tid)
                                : ((small_leaf *)last_new_l)->flush(tid);

                            children[j++] = last_new_l;

                            /*
                            NODE CREATION POINT:
                            A new sparse node is created, holding:
                            The children array, which contains pointers to the
                            new leaves, one of which holds the newly inserted
                            key-value pair, because we have called leaf::insert,
                            which inserted the pair into the tmp array which was
                            used to construct the leaves
                            */
                            GSTATS_ADD(tid, sparse_creation_to_leaves, 1);

                            sparse_node *new_s = sparse_pool.new_obj(
                                tid, byte_pos, (node **)&children[0],
                                (node **)&children[j]);

                            /*
                            NEW NODE FLUSH POINT
                            CAREFUL: there might be implied fences in the sparse
                            constructor, where child pointers (which are either
                            verlib::versioned_ptrs or std::atomic pointers) are
                            written to.
                            */
                            new_s->flush(tid);
                            /*
                            WRITE POINT:
                             The child pointer is updated to point to the new
                            sparse node
                            */
                            *cptr = (node *)new_s;

                            big_leaf_pool.retire(tid, bl);
                        }
                    } else {  // not a leaf

                        /*
                        NODE CREATION POINT
                        A new small leaf is created, holding:
                        Only the new key-value pair to be inserted
                        */
                        small_leaf *new_l = small_leaf_pool.new_obj(tid, k, v);

                        /*
                        NEW NODE FLUSH POINT
                        */
                        new_l->flush(tid);

                        GSTATS_ADD(tid, sparse_creation_to_internals, 1);

                        /*
                        NODE CREATION POINT
                        A new sparse node is created holding children:
                        1. The old node that existed at the location (can be
                        anything but a leaf), i.e. c
                        2. The new small leaf that was created above
                        */
                        sparse_node *new_s = sparse_pool.new_obj(
                            tid, byte_pos, c, c->key, (node *)new_l, k);

                        /*
                        NEW NODE FLUSH POINT
                        CAREFUL: there might be implied fences in the sparse
                        constructor, where child pointers (which are either
                        verlib::versioned_ptrs or std::atomic pointers) are
                        written to.
                        */

                        new_s->flush(tid);

                        /*
                        WRITE POINT
                        The child pointer is updated to point to the new sparse
                        node
                        */
                        *cptr = (node *)new_s;
                    }
                    return true;
                })) {
                return NO_VALUE;
            }
        } else {  // no child pointer, need to add
            bool x = add_child(tid, gp, p, k, v);
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
        // GSTATS_ADD_IX(tid, delete_attempts_histogram, 1, attempts);
        return ret;
    }

    V remove(const int tid, const K &k) {
        return verlib::with_epoch(tid, [=] { return remove_(tid, k); });
    }

    std::optional<V> try_remove(const int tid, const K &k) {
        auto [gp, p, cptr, c, byte_pos] = find_location(tid, root, k);
        if (cptr == nullptr) {  // TODO: is this correct?
            return NO_VALUE;
        }
        // if not found return
        if (c == nullptr ||
            !(c->is_leaf() &&
              ((leaf *)c)->find(tid, k, byte_pos).has_value())) {
            if (GET_LOCK(p).read_lock(
                    [&] { return (cptr->load() == c) && !p->removed.load(); }))
                return NO_VALUE;
            else
                return {};
        }
        std::optional<V> ret;
        if (GET_LOCK(p).try_lock(tid, [=, &ret] {
                if (p->removed.load() || cptr->load() != c) return false;
                leaf *l = (leaf *)c;
                ret = l->find(tid, k, byte_pos);
                if (l->size == 1) {
                    node *other_child = single_other_child(p, c);
                    //                if (other_child != nullptr && gp->nt !=
                    //                Sparse) {
                    //                    // if parent will become singleton try
                    //                    to remove parent as well return
                    //                    GET_LOCK(gp).try_lock([=] {
                    //                        auto child_ptr = get_child(gp,
                    //                        p->key); if (gp->removed.load() ||
                    //                        child_ptr->load() != p)
                    //                            return false;
                    //                        *child_ptr = other_child;
                    //                        p->removed = true;
                    //                        sparse_pool.retire((sparse_node *)
                    //                        p);
                    //                        small_leaf_pool.retire((small_leaf
                    //                        *) l); return true;
                    //                    });
                    //                } else
                    {  // just remove child

                        /*
                        WRITE POINT
                        */
                        GSTATS_ADD(tid, remove_sleaf_to_null, 1);
                        *cptr = (node *)nullptr;

                        small_leaf_pool.retire(tid, (small_leaf *)l);
                        return true;
                    }
                } else {  // at least 2 in leaf


                    if (l->size > max_medium_leaf_size + 1) {
                        // bleaf to bleaf
                        big_leaf *new_bl = big_leaf_pool.new_obj(tid, l, k);
                        new_bl->flush(tid);
                        *cptr = (node *)new_bl;
                        big_leaf_pool.retire(tid, (big_leaf *)l);
                    } else if (l->size == max_medium_leaf_size + 1) {
                        // bleaf to mleaf
                        medium_leaf *new_ml =
                            medium_leaf_pool.new_obj(tid, l, k);
                        new_ml->flush(tid);
                        *cptr = (node *)new_ml;
                        big_leaf_pool.retire(tid, (big_leaf *)l);
                    } else if (l->size > max_small_leaf_size + 1) {
                        // mleaf to mleaf
                        medium_leaf *new_ml =
                            medium_leaf_pool.new_obj(tid, l, k);
                        new_ml->flush(tid);
                        *cptr = (node *)new_ml;
                        medium_leaf_pool.retire(tid, (medium_leaf *)l);
                    } else if (l->size == max_small_leaf_size + 1) {
                        // mleaf to sleaf
                        small_leaf *new_sl = small_leaf_pool.new_obj(tid, l, k);
                        new_sl->flush(tid);
                        *cptr = (node *)new_sl;
                        medium_leaf_pool.retire(tid, (medium_leaf *)l);
                    } else {
                        // sleaf to sleaf
                        small_leaf *new_sl = small_leaf_pool.new_obj(tid, l, k);
                        new_sl->flush(tid);
                        *cptr = (node *)new_sl;
                        small_leaf_pool.retire(tid, (small_leaf *)l);
                    }

                    return true;
                }
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
                return ot(((leaf *)l)->find(0, k, pos));
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
                return ((leaf *)l)->find(tid, k, pos);
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
    static void range_internal(const int tid, node *a, AddF &add, std::optional<K> start,
                               std::optional<K> end, int pos) {
        if (a == nullptr) return;
        std::optional<K> empty;
        if (a->nt == Leaf) {
            GSTATS_ADD(tid, range_leaf_traversals, 1);
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
        for (int i = pos; i < a->byte_num; i++) {
            if (start == empty && end == empty) break;
            if (start.has_value() && String::get_byte(start.value(), i) >
                                         String::get_byte(a->key, i) ||
                end.has_value() && String::get_byte(end.value(), i) <
                                       String::get_byte(a->key, i))
                return;
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
            for (int i = sb; i <= eb; i++)
                range_internal(tid, ((full_node *)a)->children[i].read_snapshot(),
                               add, start, end, a->byte_num);
        } else if (a->nt == Indirect) {
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
                    range_internal(tid, ai->ptr[o].read_snapshot(), add, start, end,
                                   a->byte_num);
                }
#endif
            }
        } else {  // Sparse
            sparse_node *as = (sparse_node *)a;
            for (int i = 0; i < as->size; i++) {
                int b = as->keys[i];
                if (b >= sb && b <= eb)
                    range_internal(tid,as->ptr[i].read_snapshot(), add, start, end,
                                   a->byte_num);
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
        std::vector<kvpair> result;
        auto add = [&](K k, V v) { result.push_back(std::make_pair(k, v)); };
        verlib::with_snapshot(tid, [&] { range_(tid, add, lo, hi); });
        resultSize = result.size();
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
#ifdef Versioned
        std::cout << "Offset of timestamp: " << offsetof(full_node, time_stamp)
                  << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(full_node, next_version) << std::endl;
#endif
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

#ifdef Versioned
        std::cout << "Offset of time_stamp: "
                  << offsetof(indirect_node, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(indirect_node, next_version) << std::endl;
#endif
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

#ifdef Versioned
        std::cout << "Offset of time_stamp: "
                  << offsetof(sparse_node, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(sparse_node, next_version) << std::endl;
#endif
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
#ifdef Versioned
        std::cout << "Offset of time_stamp: "
                  << offsetof(small_leaf, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(small_leaf, next_version) << std::endl;
#endif
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

        std::cout << "MEDIUM LEAF NODE: " << std::endl;
        std::cout << "Size of medium_leaf: " << sizeof(medium_leaf)
                  << std::endl;
#ifdef Versioned
        std::cout << "Offset of time_stamp: "
                  << offsetof(medium_leaf, time_stamp) << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(medium_leaf, next_version) << std::endl;
#endif
        std::cout << "Offset of nt: " << offsetof(medium_leaf, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(medium_leaf, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(medium_leaf, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(medium_leaf, key)
                  << std::endl;
        std::cout << "Offset of key_vals: " << offsetof(medium_leaf, key_vals)
                  << std::endl;
        std::cout << std::endl;

        std::cout << "BIG LEAF NODE: " << std::endl;
        std::cout << "Size of big_leaf: " << sizeof(big_leaf) << std::endl;
#ifdef Versioned
        std::cout << "Offset of time_stamp: " << offsetof(big_leaf, time_stamp)
                  << std::endl;
        std::cout << "Offset of next_version: "
                  << offsetof(big_leaf, next_version) << std::endl;
#endif
        std::cout << "Offset of nt: " << offsetof(big_leaf, nt) << std::endl;
        std::cout << "Offset of size: " << offsetof(big_leaf, size)
                  << std::endl;
        std::cout << "Offset of byte_num: " << offsetof(big_leaf, byte_num)
                  << std::endl;
        std::cout << "Offset of key: " << offsetof(big_leaf, key) << std::endl;
        std::cout << "Offset of key_vals: " << offsetof(big_leaf, key_vals)
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
        medium_leaf_pool.clear();
        big_leaf_pool.clear();
    }

    static void reserve(size_t n) {}

    static void shuffle(size_t n) {
        full_pool.shuffle(n / 100);
        indirect_pool.shuffle(n / 10);
        sparse_pool.shuffle(n / 5);
        small_leaf_pool.shuffle(n);
        medium_leaf_pool.shuffle(n);
        big_leaf_pool.shuffle(n);
    }

    static void stats() {
        full_pool.stats();
        indirect_pool.stats();
        sparse_pool.stats();
        small_leaf_pool.stats();
       medium_leaf_pool.stats();
        big_leaf_pool.stats();
    }
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
verlib::memory_pool<typename ordered_map<K, V, S>::medium_leaf>
    ordered_map<K, V, S>::medium_leaf_pool;