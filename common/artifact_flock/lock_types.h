#pragma once
#include <atomic>

#include "epoch.h"
#include "no_tagged.h"
#include "durable_tools.h"

namespace flck {
    thread_local int setbench_tid = -1;

    template <typename V>
    struct atomic {
    private:
        std::atomic<V> v;

    public:
        static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value,
                      "Type for mutable must be a pointer or at most 4 bytes");
        atomic(V v) : v(v) {}
        atomic() : v(0) {}
        void init(V vv) { v = vv; }
        V load() { return v.load(); }
        V read() { return v.load(); }
        V read_snapshot() { return v.load(); }
        V read_cur() { return v.load(); }
        void store(V vv) { v = vv; }
        bool cas(V old_v, V new_v) { return (v.load() == old_v && v.compare_exchange_strong(old_v, new_v)); }
        bool cas_ni(V old_v, V new_v) { return cas(old_v, new_v); }
        void cam(V old_v, V new_v) {
            if (v.load() == old_v) v.compare_exchange_strong(old_v, new_v);
        }
        bool cam_bool(V old_v, V new_v) {
            if (v.load() == old_v) return v.compare_exchange_strong(old_v, new_v);
            return false;
        }
        V operator=(V b) {
            store(b);
            return b;
        }

        // compatibility with multiversioning
        void validate() {}
    };


    #if defined DO_ADD_UNPERSISTENT_BIT
    template <typename V>
    struct my_atomic { 
        private: 
        std::atomic<V> v; 

        public: 
        static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value, "Type for mutable must be a pointer or at most 4 bytes");

        static inline V add_unpersisted_bit(V v) { return (V) (4ul | (size_t) v); }
        static inline V strip_unpersisted_bit(V v) { return (V) ((size_t) v & ~4ul); }
        static inline bool has_unpersisted_bit(V v) { return (size_t) v & 4ul; }

        my_atomic(V v) : v(v) {}
        my_atomic() : v(0) {}
        void init(V vv) { 
            // TODO: maybe need to change this
            v = vv; 
        }

        V load_help() { 
            V x = v.load(); 
            if (has_unpersisted_bit(x)) {
                durableTools::FLUSH_LINES(setbench_tid, &v, sizeof(v));
                v.compare_exchange_strong(x, strip_unpersisted_bit(x));
            }
            return strip_unpersisted_bit(x);
        }

        #ifdef SpinUntilFlushed
        V load_wait() {
            V x = v.load();
            while (has_unpersisted_bit(x)) {
                for (int i = 0; i < SpinPauseCount; i++) {
                    _mm_pause();
                }
                x = v.load();
            }
            return x;
        }
        #endif

        inline V load() {
            #ifdef SpinUntilFlushed
            return load_wait();
            #else
            return load_help();
            #endif
        }

        V read_snapshot() { return load(); }

        void store(V vv) { 
            v = add_unpersisted_bit(vv);
            #ifdef DO_FLUSH_PTRS
            durableTools::FLUSH_LINES(setbench_tid, &v, sizeof(v));
            #endif 
            // this can be a store because in my use case i'm holding the lock
            v = strip_unpersisted_bit(vv);
        }

        V operator=(V b) {
            store(b);
            return b;
        }

    };
    #else 
    template <typename V>
    struct my_atomic {
    private:
        std::atomic<V> v;

    public:
        static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value,
                      "Type for mutable must be a pointer or at most 4 bytes");
        my_atomic(V v) : v(v) {}
        my_atomic() : v(0) {}
        void init(V vv) { v = vv; }
        V load() { return v.load(); }
        V read() { return v.load(); }
        V read_snapshot() { return v.load(); }
        V read_cur() { return v.load(); }
        void store(V vv) { 
            v = vv;
            #ifdef DO_FLUSH_PTRS
            durableTools::FLUSH_LINES(setbench_tid, &v, sizeof(v));
            #endif
        }
        bool cas(V old_v, V new_v) { return (v.load() == old_v && v.compare_exchange_strong(old_v, new_v)); }
        bool cas_ni(V old_v, V new_v) { return cas(old_v, new_v); }
        void cam(V old_v, V new_v) {
            if (v.load() == old_v) v.compare_exchange_strong(old_v, new_v);
        }
        bool cam_bool(V old_v, V new_v) {
            if (v.load() == old_v) return v.compare_exchange_strong(old_v, new_v);
            return false;
        }
        V operator=(V b) {
            store(b);
            return b;
        }

        // compatibility with multiversioning
        void validate() {}
    };


    #endif
    

    template <typename V>
    struct atomic_write_once {
        std::atomic<V> v;
        atomic_write_once(V initial) : v(initial) {}
        atomic_write_once() {}
        V load() { return v.load(); }
        V load_ni() { return v.load(); }
        V read() { return v.load(); }
        void init(V vv) { v = vv; }
        void store(V vv) { v = vv; }
        void store_ni(V vv) { v = vv; }
        bool cas_ni(V exp_v, V new_v) { return v.compare_exchange_strong(exp_v, new_v); }
        void cam(V old_v, V new_v) {
            if (v.load() == old_v) v.compare_exchange_strong(old_v, new_v);
        }
        V operator=(V b) {
            store(b);
            return b;
        }
        // inline operator V() { return load(); } // implicit conversion
    };

    template <typename V>
    using atomic_aba_free = atomic_write_once<V>;

    template <typename T>
    using memory_pool = internal::mem_pool<T>;

    // to make consistent with lock free implementation
    namespace internal {
        template <typename T>
        using tagged = no_tagged<T>;
    }

    template <typename F>
    bool skip_if_done(F f) {
        f();
        return true;
    }

    template <typename F>
    bool skip_if_done_no_log(F f) {
        f();
        return true;
    }

    template <typename V>
    V commit(V v) {
        return v;
    }

    template <typename F>
    void non_idempotent(F f) {
        f();
    }

    bool check_synchronized(long i) { return true; }

}  // namespace flck
