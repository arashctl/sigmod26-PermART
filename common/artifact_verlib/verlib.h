// This selects between using versioned objects or regular objects
// Versioned objects are implemented as described in:
//   Wei, Ben-David, Blelloch, Fatourou, Rupert and Sun,
//   Constant-Time Snapshots with Applications to Concurrent Data Structures
//   PPoPP 2021
// They support snapshotting via version chains, and without
// indirection, but pointers to objects (ptr_type) must be "recorded
// once" as described in the paper.
#pragma once
#include <artifact_parlay/parallel.h>
#include <artifact_parlay/sequence.h>

#include "artifact_flock/flock.h"

namespace verlib {
    bool strict_lock = false;
}  // namespace verlib

#ifdef Versioned

// versioned objects, ptr_type includes version chains
#ifdef Recorded_Once
#include "versioned_recorded_once.h"
#elif FullyIndirect
#include "versioned_indirect.h"
#else
#ifdef OldPlainVersionedHybrid
#include "versioned_hybrid.h"
#else
#include "versioned_hybrid_ptr_level.h"
#endif
#endif

#else  // Not Versioned

namespace verlib {
    #ifdef UseSetbenchTids
    thread_local int setbench_tid = -1;
    thread_local bool thread_is_inserting = false;
    thread_local bool measuring_cow_time = false;
    #endif 

    struct versioned {};
    struct versioned_ts_only {};

    template <typename T>
    using versioned_ptr = flck::my_atomic<T*>;

    using atomic_bool = flck::atomic<bool>;
    using flck::lock;
    using flck::memory_pool;
    template <typename A, typename B, typename C>
    bool validate(const A& a, const B& b, const C& c) {
        return true;
    }

#ifdef UseSetbenchTids
    template <typename F>
    auto with_snapshot(const int tid, F f, bool unused_parameter = false) {
        return flck::with_epoch(tid, [&] { return f(); });
    }
#else
    template <typename F>
    auto with_snapshot(F f, bool unused_parameter = false) {
        return flck::with_epoch([&] { return f(); });
    }
#endif

    template <typename F>
    auto do_now(F f) {
        return f();
    }
}  // namespace verlib

#endif

namespace verlib {
    using flck::with_epoch;
}
