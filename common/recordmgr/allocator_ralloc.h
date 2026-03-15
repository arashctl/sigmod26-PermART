/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef ALLOC_RALLOC_H
#define ALLOC_RALLOC_H

#include <cassert>
#include <cstdlib>
#include <iostream>

#include "plaf.h"
#include "pool_interface.h"

#ifdef USE_RALLOC
#include "ralloc_n0dl.hpp"
#include "ralloc_n1dl.hpp"
#endif

//__thread long long currentAllocatedBytes = 0;
//__thread long long maxAllocatedBytes = 0;

template <typename T = void>
class allocator_ralloc : public allocator_interface<T> {
    PAD;  // post padding for allocator_interface
   public:
    template <typename _Tp1>
    struct rebind {
        typedef allocator_ralloc<_Tp1> other;
    };

    inline int numa_node(const int tid) {
        if (tid < 48) {
            return 1;
        } else {
            return 0;
        }
    }

    // reserve space for ONE object of type T
    T* allocate(const int tid) {
        // allocate a new object
        MEMORY_STATS {
            this->debug->addAllocated(tid, 1);
            VERBOSE {
                if ((this->debug->getAllocated(tid) % 2000) == 0) {
                    debugPrintStatus(tid);
                }
            }
            //            currentAllocatedBytes += sizeof(T);
            //            if (currentAllocatedBytes > maxAllocatedBytes) {
            //                maxAllocatedBytes = currentAllocatedBytes;
            //            }
        }
        // return new T;  //(T*) malloc(sizeof(T));

        #ifdef USE_RALLOC

        if (numa_node(tid) == 0) {
            return (T*) RP_N0DL_malloc(sizeof(T));
        } else {
            return (T*) RP_N1DL_malloc(sizeof(T));
        }

        #else 
        return new T;  //(T*) malloc(sizeof(T));
        #endif 
        

    }

    template <class... Args>
    T* allocateWithArgs(const int tid, Args... args) {
        // GSTATS_ADD(tid, num_allocations, 1);
        return new T(args...);
    }

    void deallocate(const int tid, T* const p) {
        // note: allocators perform the actual freeing/deleting, since
        // only they know how memory was allocated.
        // pools simply call deallocate() to request that it is freed.
        // allocators do not invoke pool functions.
        MEMORY_STATS {
            this->debug->addDeallocated(tid, 1);
            //            currentAllocatedBytes -= sizeof(T);
        }


#ifdef USE_RALLOC 

        if (RP_N0DL_in_prange(p)) {
            RP_N0DL_free(p);
        } else if (RP_N1DL_in_prange(p)) {
            RP_N1DL_free(p);
        } else {
            assert(false); // should never happen
        }

#else 
#if !defined NO_FREE
        delete p;
#endif
#endif 
    }
    
    void deallocateAndClear(const int tid, blockbag<T>* const bag) {
#ifdef NO_FREE
        bag->clearWithoutFreeingElements();
#else
        while (!bag->isEmpty()) {
            T* ptr = bag->remove();
            deallocate(tid, ptr);
        }
#endif
    }

    void debugPrintStatus(const int tid) {
        //        std::cout<</*"thread "<<tid<<" "<<*/"allocated
        //        "<<this->debug->getAllocated(tid)<<" objects of size
        //        "<<(sizeof(T)); std::cout<<" ";
        ////        this->pool->debugPrintStatus(tid);
        //        std::cout<<std::endl;
    }

    void initThread(const int tid) {}
    void deinitThread(const int tid) {}

    allocator_ralloc(const int numProcesses, debugInfo* const _debug)
        : allocator_interface<T>(numProcesses, _debug) {
        VERBOSE DEBUG std::cout << "constructor allocator_ralloc" << std::endl;
    }
    ~allocator_ralloc() {
        VERBOSE DEBUG std::cout << "destructor allocator_ralloc" << std::endl;
    }
};

#endif /* ALLOC_RALLOC_H */
