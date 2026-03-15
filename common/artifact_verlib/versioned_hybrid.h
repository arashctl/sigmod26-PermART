#pragma once

#include "artifact_flock/flock.h"
#include "timestamps.h"

#ifdef UseSetbenchTids
#define CREATE_LINK(old_v, new_v) link_pool.new_obj(setbench_tid, (old_v), (new_v)) 
#define RETIRE_LINK(l) link_pool.retire(setbench_tid, (l))
#define DESTRUCT_LINK(l) link_pool.destruct(setbench_tid, (l))
#else
#define CREATE_LINK(old_v, new_v) link_pool.new_obj((old_v), (new_v)) 
#define RETIRE_LINK(l) link_pool.retire((l))
#define DESTRUCT_LINK(l) link_pool.destruct((l))
#endif 

namespace verlib {
    using flck::memory_pool;

    const TS tbd = std::numeric_limits<TS>::max() / 4;

    template <typename F>
    auto do_now(F f) {
        return f();
    }

    template <typename T>
    using atomic = flck::atomic<T>;
    using lock = flck::lock;
    using atomic_bool = flck::atomic_write_once<bool>;

    struct versioned {
        flck::atomic_write_once<TS> time_stamp;
        versioned* next_version;
        versioned() : time_stamp(tbd) {}
        versioned(versioned* next) : time_stamp(tbd), next_version(next) {}
    };

    struct ver_link : versioned {
        versioned* value;
        ver_link(versioned* next, versioned* value) : versioned{next}, value(value) {}
    };

    flck::memory_pool<ver_link> link_pool;

    template <typename V>
    struct versioned_ptr {
    private:
        flck::atomic<versioned*> v;

        // uses lowest bit of pointer to indicate whether indirect (1) or not (0)
        static versioned* add_indirect(versioned* ptr) { return (versioned*)(1ul | (size_t)ptr); };
        static versioned* strip_indirect(versioned* ptr) { return (versioned*)((size_t)ptr & ~1ul); }
        static bool is_indirect(versioned* ptr) { return (size_t)ptr & 1; }

        void shortcut(versioned* ptr) {
            // ART-specific assert
            assert(is_indirect(ptr)); // this function is only called on indirect pointers
#ifndef NoShortcut
            ver_link* ptr_ = (ver_link*)strip_indirect(ptr);
            // ART-specific assert
            assert(ptr_->value == nullptr); // only null pointers are stored with indirection 
            if (ptr_->time_stamp.load_ni() <= done_stamp) {
#ifdef NoHelp
                if (v.cas(ptr, ptr_->value)) {
                    RETIRE_LINK(ptr_);
                }
#else
                if (v.cas_ni(ptr, ptr_->value)) link_pool.retire_ni(ptr_);
#endif
            }
#endif
        }

        static V* get_ptr(versioned* ptr) {
            // ART-specific assert 
            assert(false); // never here in ART
            versioned* ptr_ = strip_indirect(ptr);
            if (!is_indirect(ptr)) return (V*)ptr_;
            return (V*)((ver_link*)ptr_)->value;
        }

        V* get_ptr_shortcut(versioned* ptr) {
            versioned* ptr_ = strip_indirect(ptr);
            if (!is_indirect(ptr)) return (V*)ptr_;
            shortcut(ptr);
            return (V*)((ver_link*)ptr_)->value;
        }

        static versioned* set_stamp(versioned* ptr) {
            versioned* ptr_ = strip_indirect(ptr);
            if (ptr != nullptr && ptr_->time_stamp.load_ni() == tbd) {
                TS t = global_stamp.get_write_stamp();
                if (ptr_->time_stamp.load_ni() == tbd) ptr_->time_stamp.cas_ni(tbd, t);
            }
            return ptr;
        }

        static versioned* set_zero_stamp(V* ptr) {
            if (ptr != nullptr && ptr->time_stamp.load_ni() == tbd) ptr->time_stamp = zero_stamp;
            return ptr;
        }

        bool cas_from_cam(versioned* old_v, versioned* new_v) {
#ifdef NoHelp
            return v.cas(old_v, new_v);
#else
            v.cam(old_v, new_v);
            return (v.load() == new_v || strip_indirect(new_v)->time_stamp.load() != tbd);
#endif
        }

    public:
        versioned_ptr() : v(nullptr) {}
        versioned_ptr(V* ptr) : v(set_zero_stamp(ptr)) {}

        ~versioned_ptr() {
            versioned* ptr = v.load();
            if (is_indirect(ptr)) {
                DESTRUCT_LINK((ver_link*)strip_indirect(ptr));
            }
        }

        void init(V* ptr) { v = set_zero_stamp(ptr); }

        V* read_snapshot() {
            TS ls = local_stamp;
            versioned* head = set_stamp(v.read());
            versioned* head_unmarked = strip_indirect(head);

            #ifdef TrackPtrChases
            GSTATS_ADD(verlib::setbench_tid, node_snapshot_reads, 1);
            int chases = 1;
            #endif 

            // chase down version chain
            while (head != nullptr && global_stamp.less(ls, head_unmarked->time_stamp.load())) {
                head = head_unmarked->next_version;
                head_unmarked = strip_indirect(head);

                #ifdef TrackPtrChases
                chases++;
                #endif
            }

            #ifdef TrackPtrChases
            GSTATS_ADD(verlib::setbench_tid, vptr_chases, chases);
            GSTATS_ADD_IX(verlib::setbench_tid, version_chain_traversal_histogram, 1, std::min(chases,9999));
            #endif 

#ifdef LazyStamp
            if (head != nullptr && global_stamp.equal(head_unmarked->time_stamp.load(), ls) && speculative)
                aborted = true;
#endif
            if (is_indirect(head)) {
                return (V*)((ver_link*)head_unmarked)->value;
            } else
                return (V*)head;
        }

        V* load() {  // can be used anywhere
            if (local_stamp != -1)
                return read_snapshot();
            else
                return get_ptr_shortcut(set_stamp(v.load()));
        }

        V* read() {  // only safe on journey
            // ART-specific assert
            assert(false); // never here in ART
            return get_ptr_shortcut(v.read());
        }

        void validate() {
            set_stamp(v.load());  // ensure time stamp is set
        }

#ifdef SlowStore
        void store(V* newv) {
            V* oldv = load();
            cas(oldv, newv);
        }
#else
        void store(V* ptr) {
            versioned* old_v = v.load();
            versioned* new_v = ptr;
            bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

            if (use_indirect) {
                // ART-specific assert
                assert(ptr == nullptr); // the only way to get here in ART is if ptr is nullptr
                new_v = add_indirect(CREATE_LINK(old_v, new_v));
            } else {
                // ART-specific assert
                assert(ptr != nullptr); // non-nullptr stores are always without indirection
                ptr->next_version = old_v;
            }

#ifdef NoShortcut
            v = new_v;
            if (is_indirect(old_v)) {
                RETIRE_LINK((ver_link*)strip_indirect(old_v));
            }
#else
            
            v.cam(old_v, new_v); // This might fail because of the shortcut
            // The only write to v that might be concurrent with me is the shortcut call by some loader
            // Another write to v is impossible, because ART is lock-protected and there are no WW conflicts

            if (is_indirect(old_v)) {
                // If v used to be indirect, it means that it might have been shortcutted out since I last read it
                // Let's check for that: 

                versioned* val = v.load(); // Load v now
                ver_link* old_l = (ver_link*)strip_indirect(old_v); // old_l->value is the place where the old link points to
                if (val != old_l->value) {
                    // Other thread's shortcut failed! I can retire the link now because my write was performed
                    assert(val == new_v); // I must have successfully written my value
                    RETIRE_LINK(old_l);
                } else { 
                    // Other thread's shortcut was successful! Try again to do my store
                    assert(val == nullptr); // I only shortcut nullptrs and nothing else
                    v.cam(val, new_v);
                }
            }
#endif
            set_stamp(new_v);
            if (use_indirect) shortcut(new_v);
        }
#endif

        bool cas(V* exp, V* ptr) {
            // ART-specific assert
            assert(false); // never here in ART
            versioned* old_v = v.load();
            versioned* new_v = ptr;
            V* old = get_ptr(old_v);
            set_stamp(old_v);
            if (old != exp) return false;
            if (exp == ptr) return true;
            bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

            if (use_indirect) {
                new_v = add_indirect(CREATE_LINK(old_v, new_v));
            } else {
                ptr->next_version = old_v;
            }

            bool succeeded = cas_from_cam(old_v, new_v);
#ifndef NoShortcut
            if (!succeeded && is_indirect(old_v)) {
                old_v = ((ver_link*)strip_indirect(old_v))->value;
                if (old_v == v.load()) succeeded = cas_from_cam(old_v, new_v);
            }
#endif
            if (succeeded) {
                set_stamp(new_v);
                if (is_indirect(old_v)) {
                    RETIRE_LINK((ver_link*)strip_indirect(old_v));
                }
#ifndef NoShortcut
                if (use_indirect) {
                    shortcut(new_v);
                }
#endif
                return true;
            }
            if (use_indirect) {
                DESTRUCT_LINK((ver_link*)strip_indirect(new_v));
            }
            set_stamp(v.load());
            return false;
        }

        bool casz(V* exp, V* ptr) {
            // ART-specific assert
            assert(false); // never here in ART
#ifndef NoShortcut
            for (int ii = 0; ii < 2; ii++) {
#endif
                versioned* old_v = v.load();
                versioned* new_v = ptr;
                V* old = get_ptr(old_v);
                set_stamp(old_v);
                if (old != exp) return false;
                if (exp == ptr) return true;
                bool use_indirect = (ptr == nullptr || ptr->time_stamp.load() != tbd);

                if (use_indirect) {
                    new_v = add_indirect(CREATE_LINK(old_v, new_v));
                }
                else
                    ptr->next_version = old_v;

                if (cas_from_cam(old_v, new_v)) {
                    set_stamp(new_v);
                    if (is_indirect(old_v)) {
                        RETIRE_LINK((ver_link*)strip_indirect(old_v));
                    }
#ifndef NoShortcut
                    if (use_indirect) shortcut(new_v);
#endif
                    return true;
                }
                if (use_indirect) {
                    DESTRUCT_LINK((ver_link*)strip_indirect(new_v));
                }
#ifndef NoShortcut
            }
#endif
            set_stamp(v.load());
            return false;
        }

        V* operator=(V* b) {
            store(b);
            return b;
        }
    };

}  // namespace verlib
