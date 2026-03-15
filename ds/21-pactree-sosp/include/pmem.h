// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#if defined(USE_PMDK)
#include <libpmemobj.h>
#elif defined(USE_RALLOC)

// Six different pools for ralloc
// I know it's ugly but it was faster to get it working this way than making a generic solution for ralloc
#include "ralloc_n0sl.hpp"
#include "ralloc_n1sl.hpp"
#include "ralloc_n0log.hpp"
#include "ralloc_n1log.hpp"
#include "ralloc_n0dl.hpp"
#include "ralloc_n1dl.hpp"

#endif 


#include <iostream>
#include <unistd.h>
#include <cassert>

#include "arch.h"

#define MASK 0x8000FFFFFFFFFFFF
#define MASK_DIRTY 0xDFFFFFFFFFFFFFFF //DIRTY_BIT
#define MASK_POOL 0x7FFFFFFFFFFFFFFF

//#define MEM_AMOUNT

#if defined(USE_PMDK)
typedef struct root_obj{
	PMEMoid ptr[2];
	//    PMEMoid ptr2;
}root_obj;

// These are mostly for debugging and statistics
void printMemAmount();
void addMemAmount(unsigned long amt);
void zeroMemAmount();

class PMDKPMem {
	private:
		static void *baseAddresses[6]; // dram
		static void* logVaddr[2];
		//	static PMEMoid logSpace[4];
	public:
		static void *getBaseOf(int poolId) {
			return baseAddresses[poolId];
		}
		static bool alloc(int poolId, size_t size, void **p) {
			// allocate a size memory from pool_id
			// and store/persist address to *p
			// return true on succeed
			PMEMobjpool *pop = (PMEMobjpool *)baseAddresses[poolId];            
			PMEMoid oid;
			int ret = pmemobj_alloc(pop, &oid, size, 0, NULL, NULL);
			if(ret){
				std::cerr<<"alloc error"<<std::endl;	
				return false;
			}
			//DEBUG
			*p= reinterpret_cast<void*> (((unsigned long)poolId) << 48 | oid.off);
			return true;
		}

		static bool alloc(int poolId, size_t size, void **p, PMEMoid *oid) {
			// allocate a size memory from pool_id
			// and store/persist address to *p
			// return true on succeed
			PMEMobjpool *pop = (PMEMobjpool *)baseAddresses[poolId];            
			int ret = pmemobj_alloc(pop, oid, size, 0, NULL, NULL);
			if(ret){
				std::cerr<<"alloc error"<<std::endl;	
				return false;
			}
			//DEBUG
			*p= reinterpret_cast<void*> (((unsigned long)poolId) << 48 | oid->off);
			return true;
		}
		static void free(void *pptr) {
			// p -> pool_id and offset
			// then perform free
			int poolId = (((unsigned long)pptr)&MASK_POOL) >> 48;
			void *rawPtr = (void *)(((unsigned long)pptr)& MASK + (unsigned long)baseAddresses[poolId]);
			PMEMoid ptr = pmemobj_oid(rawPtr);
			pmemobj_free(&ptr);
		}

		static void freeVaddr(void *vaddr) {
			// p -> pool_id and offset
			// then perform free
			PMEMoid ptr = pmemobj_oid(vaddr);
			pmemobj_free(&ptr);
		}

		// case 1
		static bool bind(int poolId, const char *nvm_path, size_t size, void **root_p, int *is_created) {
			// open nvm_path with PMDK api and associate
			PMEMobjpool *pop;
			const char* layout = "phydra";
			if (access(nvm_path, F_OK) != 0) {
				pop = pmemobj_create(nvm_path, layout, size, 0666);
				if(pop == nullptr){
					std::cerr<<"bind create error "<<"path : "<<nvm_path<<std::endl;	
					return false;
				}
				baseAddresses[poolId] = reinterpret_cast<void*>(pop);
				*is_created = 1;
			}
			else{
				pop = pmemobj_open(nvm_path,layout); 
				if(pop == nullptr){
					std::cerr<<"bind open error"<<std::endl;	
					std::cerr<<"pooId: "<<poolId<<" nvm_path : "<<nvm_path<<std::endl;	
					return false;
				}
				baseAddresses[poolId] = reinterpret_cast<void*>(pop);
			}
			PMEMoid g_root = pmemobj_root(pop, sizeof(root_obj));
			*root_p=(root_obj*)pmemobj_direct(g_root);
			zeroMemAmount();
			return true;

		}
		static bool unbind(int poolId) {
			PMEMobjpool *pop = reinterpret_cast<PMEMobjpool *>(baseAddresses[poolId]);
			pmemobj_close(pop);
			return true;
		}

		static bool bindLog(int poolId, const char *nvm_path, size_t size) {
			PMEMobjpool *pop;
			int ret;

			if (access(nvm_path, F_OK) != 0) {
				pop = pmemobj_create(nvm_path, POBJ_LAYOUT_NAME(nv), size, 0666);
				if(pop == nullptr){
					std::cerr<<"bind create error"<<std::endl;	
					return false;
				}
				baseAddresses[poolId*3+2] = reinterpret_cast<void*>(pop);
				PMEMoid g_root = pmemobj_root(pop, sizeof(PMEMoid));
				//       PMEMoid g_root = pmemobj_root(pop, 64UL*1024UL*1024UL*1024UL);
				int ret = pmemobj_alloc(pop, &g_root, 512UL*1024UL*1024UL, 0, NULL, NULL);
				if(ret){
					std::cerr<<"!!! alloc error"<<std::endl;	
					return false;
				}
				logVaddr[poolId] = pmemobj_direct(g_root);
				memset((void*)logVaddr[poolId],0,512UL*1024UL*1024UL);
			}
			else{
				//TODO FIX IT
				pop = pmemobj_open(nvm_path, POBJ_LAYOUT_NAME(nv));
				if(pop == nullptr){
					std::cerr<<"bind log open error"<<std::endl;	
					return false;
				}
				PMEMoid g_root = pmemobj_root(pop, sizeof(PMEMoid));
				baseAddresses[poolId*3+2] = reinterpret_cast<void*>(pop);
				logVaddr[poolId] = pmemobj_direct(g_root);
			}
			return true;
		}

		static bool unbindLog(int poolId) {
			PMEMobjpool *pop = reinterpret_cast<PMEMobjpool *>(baseAddresses[poolId*3+2]);
			pmemobj_close(pop);
			return true;
		}

		static void* getOpLog(int i){
			unsigned long vaddr = (unsigned long)logVaddr[0];
			//printf("vaddr :%p %p\n",vaddr, (void *)(vaddr+(64*i)));

			return (void*)(vaddr + (64*i));
		}

};

#elif defined(USE_RALLOC)


// Root objects in ralloc are plain pointers
// There's on pmemoid_direct and pmemoid_root going on
// Root object in libpmem obj is an entry point for all other persistent objects. 


// The root object contains two pointers
// They point to head of the search layer for each numa node. 
typedef struct root_obj {
	void *ptr[2];
} root_obj;

class RallocPMem {

	private: 
	static void* baseAddresses[6]; // is it ever used? currently here so it compiles
	static void* logVaddr[2];
	
	public:

	static void *getBaseOf(int poolId) {
		// This is not really used in ralloc
		assert(false);
		return nullptr;
	}

	// The poolId is sometimes provioded by PMem::alloc calls 
	// But sometimes it's hardcoded to either 0 or 1. 
	// In the pactree initialization, it's hardcoded to 0. 
	// In the linkedList initialization, it's hardcoded to 1.
	// That's probably what the author intended to make the most of the bandwidth. 
	// But what if MULTIPOOL is not defined?
	// I guess 0,1,2 are socket0, and 3,4,5 are socket1.
	// But is it really NUMA aware? I don't think so. 
	// I'll probably ignore all the pool stuff, and just use a single pool for one NUMA node, and two pools for two NUMA nodes. 
	

	// Pool id 0: Search Layer (ART)
	// Pool id 1: Data Layer (Linked List, List Node)
	// Pool id 2: Log

	// Pool id 3: Search Layer (ART) - NUMA node 1
	// Pool id 4: Data Layer (Linked List, List Node) - NUMA node 1
	// Pool id 5: Log - NUMA node 1

	static bool alloc(int poolId, size_t size, void **p) {
		// poolId of 0 is for Search Layer
		// poolId of 1 is for Data Layer
		// poolId of 2 is for Log
		// poolId of 3 is for Search Layer - NUMA node 1
		// poolId of 4 is for Data Layer - NUMA node 1
		// poolId of 5 is for Log - NUMA node 1
		// But in the code, sometimes poolId is hardcoded to 0 or 1. 
		assert(poolId == 0 || poolId == 1 || poolId == 3 || poolId == 4); // We don't allocate from log pool

		// Allocate the memory from the appropriate pool 
		void *mem = nullptr;
		if (poolId == 0) {
			mem = RP_N0SL_malloc(size);
		} else if (poolId == 1) {
			mem = RP_N0DL_malloc(size);
		} else if (poolId == 3) {
			mem = RP_N1SL_malloc(size);
		} else if (poolId == 4) {
			mem = RP_N1DL_malloc(size);
		} else {
			assert(false);
		}
		
		// Write it to the output pointer
		if (mem == nullptr) {
			std::cerr<<"alloc error"<<std::endl;
			return false;
		}
		*p = mem;
		return true;
	}

	static bool alloc(int poolId, size_t size, void **p, void **oid) { // different signature as we're moving away from PMemoid


		assert(poolId == 0 || poolId == 1 || poolId == 3 || poolId == 4); // We don't allocate from log pool

		// Allocate the memory from the appropriate pool
		void *mem = nullptr;
		if (poolId == 0) {
			mem = RP_N0SL_malloc(size);
		} else if (poolId == 1) {
			mem = RP_N0DL_malloc(size);
		} else if (poolId == 3) {
			mem = RP_N1SL_malloc(size);
		} else if (poolId == 4) {
			mem = RP_N1DL_malloc(size);
		} else {
			assert(false);
		}

		// Write it to the output pointer
		if (mem == nullptr) {
			std::cerr<<"alloc error"<<std::endl;
			return false;
		}

		*p = mem;

		// What to do with oid? Where is it used?
		// Let's also write to there too 
		*oid = mem;

		return true;

	}

	static inline uint16_t getPoolId(void *pptr) {
		if (RP_N0DL_in_prange(pptr)) {
			return 1;
		}

		if (RP_N1DL_in_prange(pptr)) {
			return 4;
		}

		if (RP_N0SL_in_prange(pptr)) {
			return 0;
		}

		if (RP_N1SL_in_prange(pptr)) {
			return 3;
		}

		if (RP_N0LOG_in_prange(pptr)) {
			return 2;
		}

		if (RP_N1LOG_in_prange(pptr)) {
			return 5;
		}

		assert(false); // Should never happen
		return -1;
	}

	static void freeralloc(void *pptr) {

		auto pool_id = getPoolId(pptr);
		assert(pool_id != -1);


		switch (pool_id) {
			case 0:
				RP_N0SL_free(pptr);
				break;
			case 1:
				RP_N0DL_free(pptr);
				break;
			case 2:
				// I doubt that we ever free a log entry
				assert(false);
				RP_N0LOG_free(pptr);
				break;
			case 3:
				RP_N1SL_free(pptr);
				break;
			case 4:
				RP_N1DL_free(pptr);
				break;
			case 5:
				// I doubt that we ever free a log entry
				assert(false);
				RP_N1LOG_free(pptr);
				break;
			default:
				assert(false);
		}
		return;

	}

	static inline void free(void *pptr) {
		// There's no difference between free and freeVaddr in ralloc
		freeralloc(pptr);
	}

	static inline void freeVaddr(void *vaddr) {
		// There's no difference between free and freeVaddr in ralloc
		freeralloc(vaddr);
	}

	// Called in init functions
	static bool bind(int poolId, const char *nvm_path, size_t size, void **root_p, int *is_created) {
		// bind does nothing for ralloc. So it Should never be called if using ralloc.
		assert(false);
		return false;
	}

	// Never called anywhere
	static bool unbind(int poolId) {
		assert(false);
		return false;
	}

	// Called in initPT
	static bool bindLog(int poolId, const char *nvm_path, size_t size) {

		// bindLog for PMDK allocates a huge chunk of memory for logs
		// And then PMem::getOpLog(idx) returns a pointer to the idx-th log entry
		// In ralloc, let's replicate that behavior

		// This function ignores nvm_path 

		if (poolId == 0) {
			// Socket 0
			logVaddr[0] = RP_N0LOG_malloc(size);
		} else if (poolId == 1) {
			// Socket 1
			// I don't know why logVaddr[1] is never used except for initialization
			// Probably a flaw in the original code
			// Or maybe becasue it makes sense to have only one log area globally.
			logVaddr[1] = RP_N1LOG_malloc(size);
		} else {
			assert(false);
		}

		
		return false;
	}

	// Never called anywhere
	static bool unbindLog(int poolId) {
		assert(false);
		return false;
	}

	// Called in linkedList.cpp and listNode.cpp
	static void* getOpLog(int i){
		// Returns a pointer to a log entry (the larger the i, the further away it is)
		unsigned long vaddr = (unsigned long)logVaddr[0];

		auto ptrToReturn = (void *)(vaddr + (64*i));

		return ptrToReturn;
	}


}; 

#endif 


#if defined(USE_RALLOC)
using PMem = RallocPMem;
#elif defined(USE_PMDK) 
using PMem = PMDKPMem;
#endif 

static inline void flushToNVM(char *data, size_t sz){
	volatile char *ptr = (char *)((unsigned long)data & ~(L1_CACHE_BYTES- 1));
	for (; ptr < data + sz; ptr += L1_CACHE_BYTES) {
		clwb(ptr);
	}
}

