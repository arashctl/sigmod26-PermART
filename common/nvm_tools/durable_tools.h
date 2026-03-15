#pragma once

#include <emmintrin.h>

#include "gstats.h"

#define CLSIZE 64
#define CLMASK 63

namespace durableTools {

static inline void FLUSH_LINE(void volatile *p) {
#ifndef DISABLE_FLUSHING
#if defined(USE_CLFLUSHOPT)
    asm volatile("clflushopt (%0)" ::"r"(p));
#elif defined(USE_CLWB)
    asm volatile("clwb (%0)" ::"r"(p));
#else
    asm volatile("clflush (%0)" ::"r"(p));
#endif
#else
#endif
}

static inline int FLUSH_LINES(const int tid, void volatile *p, size_t len) {
#ifdef USE_MSYNC
    msync((void *)p, len, MS_SYNC);
    return 0;
#else

#ifndef OLD_WRONG_FLUSH
    int flushCount = 0;
    char *start = (char *)p;
    char *end =
        (char *)(start + len - 1);  // inclusive end to fix the bug that caused
                                    // one extra flush (See issue #16 on GH)
    char *start_aligned = (char *)(((size_t)start) & ~(CLMASK));
    char *end_aligned = (char *)(((size_t)end) & ~(CLMASK));
#ifdef ONE_FLUSH_FEWER
    if (start_aligned + CLSIZE <= end_aligned) {
        // if there are at least two cachelines to flush
        // skip the first one
        start_aligned += CLSIZE;
    }
#endif
    while (start_aligned <= end_aligned) {
        FLUSH_LINE(start_aligned);
        GSTATS_ADD(tid, cacheline_flushes, 1);
        start_aligned += CLSIZE;
        flushCount++;
    }
#else
    int flushCount = 0;
    for (size_t i = 0; i < len; i += CLSIZE) {
        GSTATS_ADD(tid, cacheline_flushes, 1);
        FLUSH_LINE((char *)p + i);
        flushCount++;
    }
#endif
    return flushCount;
#endif
}

static inline void FLUSH_LINES(const int tid, void volatile *p, size_t start,
                               size_t end) {
    end = end - 1;  // inclusive end to fix the bug that caused one extra flush
                    // (See issue #16 on GH)
    char *start_aligned = (char *)(((size_t)start) & ~(CLMASK));
    char *end_aligned = (char *)(((size_t)end) & ~(CLMASK));
    do {
        FLUSH_LINE(start_aligned);
        GSTATS_ADD(tid, cacheline_flushes, 1);
        start_aligned += CLSIZE;
    } while (start_aligned <= end_aligned);
}

static inline void CLFLUSHOPT(void volatile *p) {
#ifndef DISABLE_FLUSHING
    asm volatile("clflushopt (%0)" ::"r"(p));
#else
#endif
}

static inline void CLWB(void volatile *p) {
#ifndef DISABLE_FLUSHING
    asm volatile("clwb (%0)" ::"r"(p));
#else
#endif
}

static inline void CLFLUSH(void volatile *p) {
#ifndef DISABLE_FLUSHING
    asm volatile("clflush (%0)" ::"r"(p));
#else
#endif
}

static inline void SFENCE() {
#ifndef DISABLE_FLUSHING
    asm volatile("sfence" ::: "memory");
#else
#endif
}

};  // namespace durableTools
