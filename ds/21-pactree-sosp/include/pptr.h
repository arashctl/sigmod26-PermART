// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "pmem.h"

template <typename T>
class pmdk_pptr {
   public:
    // TEST
    pmdk_pptr() noexcept : rawPtr{} {}
    pmdk_pptr(int poolId, unsigned long offset) {
        rawPtr = ((unsigned long)poolId) << 48 | offset;
    }
    T *operator->() {
        int poolId = (rawPtr & MASK_POOL) >> 48;
        void *baseAddr = PMem::getBaseOf(poolId);
        unsigned long offset = rawPtr & MASK;
        return (T *)((unsigned long)baseAddr + offset);
    }

    T *getVaddr() {
        unsigned long offset = rawPtr & MASK;
        if (offset == 0) { return nullptr; }

        int poolId = (rawPtr & MASK_POOL) >> 48;
        void *baseAddr = PMem::getBaseOf(poolId);

        return (T *)((unsigned long)baseAddr + offset);
    }

    unsigned long getRawPtr() { return rawPtr; }

    unsigned long setRawPtr(void *p) { rawPtr = (unsigned long)p; }

    inline void markDirty() { rawPtr = ((1UL << 61) | rawPtr); }

    bool isDirty() { return (((1UL << 61) & rawPtr) == (1UL << 61)); }

    inline void markClean() { rawPtr = (rawPtr & MASK_DIRTY); }

   private:
    unsigned long rawPtr;  // 16b + 48 b // nvm
};

template <typename T>
class ralloc_pptr {
   public:
    ralloc_pptr() noexcept : rawPtr{} {}

    ralloc_pptr(int poolId, unsigned long offset) {
        // No such thing as poolId in Ralloc
        rawPtr = offset;
    }

    T *operator->() {
        return (T *)rawPtr;
        // unsigned long offset = rawPtr;
        // if (offset == 0) { return nullptr; }

        // // No such thing as poolId in Ralloc
        // // It's probably going to return 0 all the time
        // void *baseAddr = PMem::getBaseOf(0);

        // return (T *)((unsigned long)baseAddr + offset);
    }

    T *getVaddr() {
        // The Vaddr in ralloc is the same as the -> operator
        return (T *)rawPtr;

        // unsigned long offset = rawPtr;
        // if (offset == 0) { return nullptr; }

        // // No such thing as poolId in Ralloc
        // // It's probably going to return 0 all the time
        // void *baseAddr = PMem::getBaseOf(0);

        // return (T *)((unsigned long)baseAddr + offset);
    }

    unsigned long getRawPtr() { return rawPtr; }

    unsigned long setRawPtr(void *p) { rawPtr = (unsigned long)p; }

    inline void markDirty() { rawPtr = ((1UL << 61) | rawPtr); }

    bool isDirty() { return (((1UL << 61) & rawPtr) == (1UL << 61)); }

    inline void markClean() { rawPtr = (rawPtr & MASK_DIRTY); }

   private:
    unsigned long rawPtr;  // 16b + 48 b // nvm
};

template <typename T>
#ifdef USE_RALLOC

using pptr = ralloc_pptr<T>;

#else

using pptr = pmdk_pptr<T>;

#endif