#pragma once

#include <iostream>
#include "artifact_flock/flock.h"
#include "durable_tools.h"
#include "timestamps.h"
#include "gstats.h"

#ifdef UseSetbenchTids
#define CREATE_LINK(old_v, new_v) \
    link_pool.new_obj(setbench_tid, (old_v), (new_v))
#define RETIRE_LINK(l) link_pool.retire(setbench_tid, (l))
#define DESTRUCT_LINK(l) link_pool.destruct(setbench_tid, (l))
#else
// Empty because this combination of flags is never used
#endif

namespace verlib {
using flck::memory_pool;

const TS tbd = std::numeric_limits<TS>::max() / 4;

template <typename T>
using atomic = flck::atomic<T>;
using lock = flck::lock;
using atomic_bool = flck::atomic_write_once<bool>;


long vrlb_rdtsc() {
    unsigned int lo, hi, aux;
    __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux));
    return (long)(((uint64_t)(hi & ~(1 << 31)) << 32) | lo);
}



struct versioned {
    flck::atomic_write_once<TS> time_stamp;
    versioned* next_version;
    versioned() : time_stamp(tbd) {}
    versioned(versioned* next) : time_stamp(tbd), next_version(next) {}
};

struct versioned_ts_only {
    flck::atomic_write_once<TS> time_stamp;
    versioned_ts_only() : time_stamp(tbd) {}


    #ifdef DONT_STAMP_LOGS 
    inline bool is_stamped() { return true; }
    inline void set_stamp() { }
    #else
    inline bool is_stamped() { return time_stamp.load_ni() != tbd; }
    inline void set_stamp() { 
        bool cas_success = false;
        #ifdef MEASURE_COW_TIME 
        long start; 
        if (verlib::measuring_cow_time) {
            start = vrlb_rdtsc();
        }
        #endif
        if (time_stamp.load_ni() == tbd) {
            TS t = global_stamp.get_write_stamp();
            if (time_stamp.load_ni() == tbd) {
                cas_success = time_stamp.cas_ni(tbd, t);
            }
        }
        #ifdef MEASURE_COW_TIME
        if (verlib::measuring_cow_time && cas_success) {
            auto end = vrlb_rdtsc();
            auto duration = end - start;
            GSTATS_ADD(0, insert_time_log_timestamp, duration);
        }
        #endif
    }
    #endif
};

struct ver_link : versioned {
    versioned* value;
    ver_link(versioned* next, versioned* value)
        : versioned{next}, value(value) {}
};

flck::memory_pool<ver_link> link_pool;

#if defined DO_ADD_UNPERSISTENT_BIT
template <typename V>
struct versioned_ptr {
   private:
    flck::atomic<versioned*> v;

    static inline versioned* add_unpersisted_bit(versioned* ptr) {
        return (versioned*)(4ul | (size_t)ptr);
    }
    static inline versioned* strip_unpersisted_bit(versioned* ptr) {
        return (versioned*)((size_t)ptr & ~4ul);
    }
    static inline bool has_unpersisted_bit(versioned* ptr) {
        return (size_t)ptr & 4ul;
    }

    // uses lowest bit of pointer to indicate whether indirect (1) or not (0)
    static versioned* add_indirect(versioned* ptr) {
        return (versioned*)(1ul | (size_t)ptr);
    };
    static versioned* strip_indirect(versioned* ptr) {
        return (versioned*)((size_t)ptr & ~1ul);
    }
    static bool is_indirect(versioned* ptr) { return (size_t)ptr & 1; }

#ifdef NoShortcut
    void shortcut(versioned* ptr) {}
#else
    void shortcut(versioned* ptr) {
        assert(is_indirect(
            ptr));  // this function is only called on indirect pointers
        assert(!has_unpersisted_bit(
            ptr));  // pointers with unpersisted bit are not shortcutted until
                    // they are persisted
        ver_link* ptr_ = (ver_link*)strip_indirect(ptr);
        assert(ptr_->value ==
               nullptr);  // only null pointers are stored with indirection
        if (ptr_->time_stamp.load_ni() <= done_stamp) {
            /* v WRITE POINT */
            if (v.cas(ptr, ptr_->value)) { RETIRE_LINK(ptr_); }
        }
    }

    bool shortcut_writepath(versioned* ptr) {
        assert(is_indirect(
            ptr));  // this function is only called on indirect pointers
        ver_link* ptr_ = (ver_link*)strip_indirect(ptr);
        assert(ptr_->value ==
               nullptr);  // only null pointers are stored with indirection
        if (ptr_->time_stamp.load_ni() <= done_stamp) {
            /* v WRITE POINT */
            if (v.cas(add_unpersisted_bit(ptr), ptr_->value)) {
                RETIRE_LINK(ptr_);
                return true;
            }
        }
        return false;
    }
#endif

    static V* get_ptr(versioned* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return nullptr;
    }

    V* get_ptr_shortcut(versioned* ptr) {
        versioned* ptr_ = strip_indirect(ptr);
        if (!is_indirect(ptr)) return (V*)ptr_;
        shortcut(ptr);
        return (V*)((ver_link*)ptr_)->value;
    }

    static versioned* set_stamp(versioned* ptr) {
        bool cas_success = false;
        #ifdef MEASURE_COW_TIME
        long start; 
        if (verlib::measuring_cow_time) {
            start = vrlb_rdtsc();
        }
        #endif
        versioned* ptr_ = strip_indirect(ptr);
        if (ptr != nullptr && ptr_->time_stamp.load_ni() == tbd) {
            TS t = global_stamp.get_write_stamp();
            if (ptr_->time_stamp.load_ni() == tbd) {
                cas_success = ptr_->time_stamp.cas_ni(tbd, t);
            }
        }
        #ifdef MEASURE_COW_TIME
        if (verlib::measuring_cow_time && cas_success) {
            auto end = vrlb_rdtsc();
            auto duration = end - start;
            if (verlib::thread_is_inserting) {
                GSTATS_ADD(verlib::setbench_tid, insert_time_node_timestamp, duration);
            }
        }
        #endif
        return ptr;
    }

    static versioned* set_zero_stamp(V* ptr) {
        if (ptr != nullptr && ptr->time_stamp.load_ni() == tbd) {
            ptr->time_stamp = zero_stamp;
        }
        return ptr;
    }

    bool cas_from_cam(versioned* old_v, versioned* new_v) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }

   public:
    versioned_ptr() : v(nullptr) {}
    versioned_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

    ~versioned_ptr() {
        versioned* ptr = v.load();
        if (is_indirect(ptr)) { DESTRUCT_LINK((ver_link*)strip_indirect(ptr)); }
    }

    void init(V* ptr) { v = set_zero_stamp(ptr); }


    
    // Multiplexer between old and new read_snapshot implementations
    V* read_snapshot() {
        #ifdef SpinUntilFlushed
            return read_snapshot_with_unpersisted_bit_wait();
        #else
            assert(false); // never here in ART
            return old_read_snapshot();
        #endif
    }

    V* old_read_snapshot() {
        TS ls = local_stamp;
        versioned* head = set_stamp(v.read());
        versioned* head_unmarked = strip_indirect(head);
        GSTATS_ADD(verlib::setbench_tid, node_snapshot_reads, 1);

        #ifdef TrackPtrChases
        int chases = 1;
        #endif

        // chase down version chain
        while (head != nullptr &&
               global_stamp.less(ls, head_unmarked->time_stamp.load())) {
            head = head_unmarked->next_version;
            head_unmarked = strip_indirect(head);

            #ifdef TrackPtrChases
            chases++;
            #endif
        }
        GSTATS_ADD(verlib::setbench_tid, vptr_chases, chases);
        
        #ifdef TrackPtrChases
        GSTATS_ADD_IX(verlib::setbench_tid, version_chain_traversal_histogram, 1, std::min(chases,9999));
        #endif

        // LazyStamp codepath omitted here

        if (is_indirect(head)) {
            return (V*)((ver_link*)head_unmarked)->value;
        } else {
            return (V*)head;
        }
    }

    V* read_snapshot_with_unpersisted_bit_wait() {

        TS ls = local_stamp;
        versioned* head = v.load();

        while (has_unpersisted_bit(head)) {
            for (int i = 0; i < SpinPauseCount; i++) { _mm_pause(); }
            head = v.load();
        }

        head = set_stamp(head);
        versioned* head_unmarked = strip_indirect(head);
        GSTATS_ADD(verlib::setbench_tid, node_snapshot_reads, 1);

        #ifdef TrackPtrChases
        int chases = 1;
        #endif

        #ifdef DontChaseVersionChains 
        if (is_indirect(head)) {
            return (V*)((ver_link*)head_unmarked)->value;
        } else {
            return (V*)head;
        }
        #endif 

        // Chase down version chain
        while (head != nullptr &&
                 global_stamp.less(ls, head_unmarked->time_stamp.load())) {
            head = head_unmarked->next_version;
            head_unmarked = strip_indirect(head);

            #ifdef TrackPtrChases
            chases++;
            #endif
        }
        GSTATS_ADD(verlib::setbench_tid, vptr_chases, chases);

        #ifdef TrackPtrChases
        GSTATS_ADD_IX(verlib::setbench_tid, version_chain_traversal_histogram, 1, std::min(chases,9999));
        #endif

        // LazyStamp codepath omitted here

        if (is_indirect(head)) {
            return (V*)((ver_link*)head_unmarked)->value;
        } else {
            return (V*)head;
        }
    }

    inline V* load_help() { return nullptr; }

    inline V* load_wait() {
        versioned* x = v.load();

        #ifdef READ_UNPERSISTENT_DATA // For ablation studies only

        if (has_unpersisted_bit(x)) {
            x = strip_unpersisted_bit(x);
        }

        #else 
        while (has_unpersisted_bit(x)) {
            for (int i = 0; i < SpinPauseCount; i++) { _mm_pause(); }
            x = v.load();
        }
        #endif 
        x = get_ptr_shortcut(set_stamp(
            x));  // This set stamp will fail because of the unpersisted bit
                  // wait. Maybe I could still help set the timestamp?
        assert(!has_unpersisted_bit(
            x));  // I should never see a value with unpersisted bit set
        return (V*)x;
    }

    V* load() {
        if (local_stamp != -1) {
#ifdef SpinUntilFlushed
            return read_snapshot_with_unpersisted_bit_wait();
#else
            assert(false);  // never here in ART
            return nullptr;
#endif
        } else {
#ifdef SpinUntilFlushed
            return load_wait();
#else
            return load_help();
#endif
        }
    }

    V* read() {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return nullptr;
    }

    void store(V* ptr) {
        versioned* old_v = v.load();
        versioned* new_v = ptr;
        bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

        if (use_indirect) {
            assert(ptr == nullptr);  // the only way to get here in ART is if
                                     // ptr is nullptr
            new_v = add_indirect(CREATE_LINK(old_v, new_v));
        } else {
            assert(ptr != nullptr);  // the only way to get here in ART is if
                                     // ptr is not nullptr
            ptr->next_version = old_v;
        }

#ifdef NoShortcut
        v = new_v;
        if (is_indirect(old_v)) {
            RETIRE_LINK((ver_link*)strip_indirect(old_v));
        }
#else
        /* v WRITE POINT */
        v.cam(old_v, new_v);  // no write-write conflict in ART
        // The outcome of the cam above:
        // 1. Either succeeds and my write is successful
        // 2. Or fails BECAUSE some other thread is shortcutting old_v
        // The if statement below is to handle case 2
        if (is_indirect(old_v)) {
            versioned* val =
                v.load();  // What ended up being written to v. It's either my
                           // write from above or the other thread's shortcut
            ver_link* old_l = (ver_link*)strip_indirect(
                old_v);  // The link that was possibly shortcutted
            if (val !=
                old_l->value) {  // The shortcut failed (== my write succeeded)
                assert(val == new_v);  // The only way for the shortcut to fail
                                       // is my write to succeed
                RETIRE_LINK(old_l);
            } else {  // The shortcut succeeded (== my write failed) => try
                      // again
                // val is the old value that is now shortcutted
                assert(val == nullptr);  // because I only shortcut nullptrs and
                                         // nothing else
                /* v WRITE POINT */
                v.cam(val, new_v);
            }
        }
// If old_v is not indirect, the cam above is guaranteed to succeed (no WW
// conflicts)
#endif
        set_stamp(new_v);
        if (use_indirect) { shortcut(new_v); }
    }

    void store_with_unpersisted_bit(V* ptr) {
        versioned* old_v = v.load();

        // I will not see a value that has an unpersisted bit set because I'm
        // holding the lock I will only see the value after the unpersisted bit
        // has been cleared
        assert(!has_unpersisted_bit(old_v));

        versioned* new_v = ptr;
        bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

        if (use_indirect) {
            assert(ptr == nullptr);  // the only way to get here in ART is if
                                     // ptr is nullptr
            new_v = add_indirect(CREATE_LINK(old_v, new_v));
        } else {
            assert(ptr != nullptr);  // the only way to get here in ART is if
                                     // ptr is not nullptr
            ptr->next_version = old_v;
        }

        versioned* new_v_marked = add_unpersisted_bit(new_v);

#ifdef NoShortcut
        v = new_v;
        if (is_indirect(old_v)) {
            RETIRE_LINK((ver_link*)strip_indirect(old_v));
        }
#else
        /* v WRITE POINT */
        v.cam(old_v, new_v_marked);  // no write-write conflict in ART
        // The outcome of the cam above:
        // 1. Either succeeds and my write is successful
        // 2. Or fails BECAUSE some other thread is shortcutting old_v
        // The if statement below is to handle case 2
        if (is_indirect(old_v)) {
            versioned* val =
                v.load();  // What ended up being written to v. It's either my
                           // write from above or the other thread's shortcut
            ver_link* old_l = (ver_link*)strip_indirect(
                old_v);  // The link that was possibly shortcutted
            // TODO: think about this if statement
            if (val !=
                old_l->value) {  // The shortcut failed (== my write succeeded)
                assert(val == new_v_marked);  // The only way for the shortcut
                                              // to fail is my write to succeed
                RETIRE_LINK(old_l);
            } else {  // The readpath shortcut succeeded (== my write failed) =>
                      // try again
                // val is the old value that is now shortcutted
                assert(
                    val ==
                    nullptr);  // because I only shortcut nullptrs and nothing
                               // else, and shortcutted nullptrs don't have
                               // unpersisted bit because I can recognize them
                               // by DRAM/PMEM address space difference
                /* v WRITE POINT */
                v.cam(val, new_v_marked);
            }
        }
// If old_v is not indirect, the cam above is guaranteed to succeed (no WW
// conflicts)
#endif
        // At this point, the unpersisted bit is set
        // So all the readers will see the unpersisted bit and wait for it to be
        // cleared Nobody is trying to shortcut the value out because:
        // 1. If they've seen the bit, they're waiting
        // 2. If they haven't seen the bit and are in the shortcutting codepath,
        // their cas will fail
        set_stamp(new_v);  // TODO: think about this, maybe a store suffices
                           // instead of an expensive cas
        bool shortcutted = false;
        if (use_indirect) { shortcutted = shortcut_writepath(new_v); }

        // What am I flushing here?
        // If I'm writing a pointer to a new node, use_indirect is false, so I'm
        // flushing the pointer to the new node If I'm writing a nullptr,
        // use_indirect is true, there are two cases:
        // 1. There are no range queries running, so I shortcutted the nullptr
        // out and that's what I'm flushing
        // 2. There are range queries running, so I couldn't perform the
        // shortcut, so I'm flushing the pointer to DRAM's ver_link that points
        // to nullptr There is NO case where someone else shortcutted the
        // nullptr out, because the reader shortcuts wait until the writers are
        // done flushing

#ifdef DO_FLUSH_PTRS
        durableTools::FLUSH_LINES(setbench_tid, &v, sizeof(v));
#endif

        // Now, readers are waiting for the unpersisted bit to be cleared
        // And there are no other writes to v that can happen concurrently with
        // me So I can clear the unpersisted bit now But I have to be careful
        // because of the shortcutting nuance

        if (use_indirect) {
            // Might be dealing with a shortcutted OR non-shortcutted nullptr
            if (shortcutted) {
                // I'm dealing with a shortcutted nullptr
                // I can do nothing because the write to nullptr was already
                // done by the shortcut function v = nullptr;
            } else {
                // I'm dealing with a non-shortcutted nullptr
                // I can clear the unpersisted bit now
                v = new_v;
            }

        } else {
            // I'm dealing with a direct pointer
            // I can clear the unpersisted bit now
            v = new_v;
        }
    }

    V* operator=(V* b) {
        store_with_unpersisted_bit(b);
        return b;
    }

    bool cas(V* exp, V* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }

    bool casz(V* exp, V* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }
};

#else

template <typename V>
struct versioned_ptr {
   private:
    flck::atomic<versioned*> v;

    // uses lowest bit of pointer to indicate whether indirect (1) or not (0)
    static versioned* add_indirect(versioned* ptr) {
        return (versioned*)(1ul | (size_t)ptr);
    };
    static versioned* strip_indirect(versioned* ptr) {
        return (versioned*)((size_t)ptr & ~1ul);
    }
    static bool is_indirect(versioned* ptr) { return (size_t)ptr & 1; }

#ifdef NoShortcut
    void shortcut(versioned* ptr) {}
#else
    void shortcut(versioned* ptr) {
        // TODO: Handle unpersisted bit here
        assert(is_indirect(
            ptr));  // this function is only called on indirect pointers
        // assert(!has_unpersisted_bit(ptr));  // pointers with unpersisted bit
        // are not shortcutted until they are persisted
        ver_link* ptr_ = (ver_link*)strip_indirect(ptr);
        assert(ptr_->value ==
               nullptr);  // only null pointers are stored with indirection
        if (ptr_->time_stamp.load_ni() <= done_stamp) {
            /* v WRITE POINT */
            if (v.cas(ptr, ptr_->value)) { RETIRE_LINK(ptr_); }
        }
    }

#endif

    static V* get_ptr(versioned* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return nullptr;
    }

    V* get_ptr_shortcut(versioned* ptr) {
        // TODO: Handle unpersisted bit here
        versioned* ptr_ = strip_indirect(ptr);
        if (!is_indirect(ptr)) return (V*)ptr_;
        shortcut(ptr);
        return (V*)((ver_link*)ptr_)->value;
    }

    static versioned* set_stamp(versioned* ptr) {

        #ifdef MEASURE_COW_TIME
        long start;
        if (verlib::measuring_cow_time) {
            start = vrlb_rdtsc();
        }
        #endif

        versioned* ptr_ = strip_indirect(ptr);
        if (ptr != nullptr && ptr_->time_stamp.load_ni() == tbd) {
            TS t = global_stamp.get_write_stamp();
            if (ptr_->time_stamp.load_ni() == tbd) {
                ptr_->time_stamp.cas_ni(tbd, t);
            }
        }

        #ifdef MEASURE_COW_TIME
        if (verlib::measuring_cow_time) {
            auto end = vrlb_rdtsc();
            auto duration = end - start;
            if (verlib::thread_is_inserting) {
                GSTATS_ADD(verlib::setbench_tid, insert_time_node_timestamp, duration);
                
            }
        }

            
        #endif

        return ptr;
    }

    static versioned* set_zero_stamp(V* ptr) {
        // TODO: Handle unpersisted bit here
        if (ptr != nullptr && ptr->time_stamp.load_ni() == tbd) {
            ptr->time_stamp = zero_stamp;
        }
        return ptr;
    }

    bool cas_from_cam(versioned* old_v, versioned* new_v) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }

   public:
    versioned_ptr() : v(nullptr) {}
    versioned_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

    ~versioned_ptr() {
        versioned* ptr = v.load();
        if (is_indirect(ptr)) { DESTRUCT_LINK((ver_link*)strip_indirect(ptr)); }
    }

    void init(V* ptr) { v = set_zero_stamp(ptr); }

    V* read_snapshot() {
        TS ls = local_stamp;
        versioned* head = set_stamp(v.read());
        versioned* head_unmarked = strip_indirect(head);
        GSTATS_ADD(verlib::setbench_tid, node_snapshot_reads, 1);

        #ifdef TrackPtrChases
        int chases = 1;
        #endif

        // chase down version chain
        while (head != nullptr &&
               global_stamp.less(ls, head_unmarked->time_stamp.load())) {
            head = head_unmarked->next_version;
            head_unmarked = strip_indirect(head);

            #ifdef TrackPtrChases
            chases++;
            #endif
        }
        GSTATS_ADD(verlib::setbench_tid, vptr_chases, chases);

        #ifdef TrackPtrChases
        GSTATS_ADD_IX(verlib::setbench_tid, version_chain_traversal_histogram, 1, std::min(chases,9999));
        #endif

        // LazyStamp codepath omitted here

        if (is_indirect(head)) {
            return (V*)((ver_link*)head_unmarked)->value;
        } else {
            return (V*)head;
        }
    }


    V* load() {
        // can be used anywhere
        if (local_stamp != -1) {
            return read_snapshot();
        } else {
            return get_ptr_shortcut(set_stamp(v.load()));
        }
    }

    V* read() {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return nullptr;
    }

    void store(V* ptr) {
        versioned* old_v = v.load();
        versioned* new_v = ptr;
        bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

        if (use_indirect) {
            assert(ptr == nullptr);  // the only way to get here in ART is if
                                     // ptr is nullptr
            new_v = add_indirect(CREATE_LINK(old_v, new_v));
        } else {
            assert(ptr != nullptr);  // the only way to get here in ART is if
                                     // ptr is not nullptr
            ptr->next_version = old_v;
        }

#ifdef NoShortcut
        v = new_v;
        if (is_indirect(old_v)) {
            RETIRE_LINK((ver_link*)strip_indirect(old_v));
        }
#else
        /* v WRITE POINT */
        v.cam(old_v, new_v);  // no write-write conflict in ART
        // The outcome of the cam above:
        // 1. Either succeeds and my write is successful
        // 2. Or fails BECAUSE some other thread is shortcutting old_v
        // The if statement below is to handle case 2
        if (is_indirect(old_v)) {
            versioned* val =
                v.load();  // What ended up being written to v. It's either my
                           // write from above or the other thread's shortcut
            ver_link* old_l = (ver_link*)strip_indirect(
                old_v);  // The link that was possibly shortcutted
            if (val !=
                old_l->value) {  // The shortcut failed (== my write succeeded)
                assert(val == new_v);  // The only way for the shortcut to fail
                                       // is my write to succeed
                RETIRE_LINK(old_l);
            } else {  // The shortcut succeeded (== my write failed) => try
                      // again
                // val is the old value that is now shortcutted
                assert(val == nullptr);  // because I only shortcut nullptrs and
                                         // nothing else
                /* v WRITE POINT */
                v.cam(val, new_v);
            }
        }
// If old_v is not indirect, the cam above is guaranteed to succeed (no WW
// conflicts)
#endif
        set_stamp(new_v);
        if (use_indirect) { shortcut(new_v); }

#ifdef DO_FLUSH_PTRS
        durableTools::FLUSH_LINES(setbench_tid, &v, sizeof(v));
#endif
    }

    V* operator=(V* b) {
        store(b);
        return b;
    }

    bool cas(V* exp, V* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }

    bool casz(V* exp, V* ptr) {
        // omitted because not used for lock-based data structures
        assert(false);  // never here in ART
        return false;
    }
};

#endif

};  // namespace verlib
