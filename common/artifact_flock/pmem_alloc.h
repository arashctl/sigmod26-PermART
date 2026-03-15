#ifndef SIMPLE_PMEM_ALLOC_H
#define SIMPLE_PMEM_ALLOC_H

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

// #define LOG_PAGE_INIT
#ifdef LOG_PAGE_INIT
#include <fstream>
#include <iostream>
#endif

// #define ALLOC_DEBUG
// #define ALLOC_DEBUG2

#ifndef PAD
#define CAT2(x, y) x##y
#define CAT(x, y) CAT2(x, y)
#define PAD volatile char CAT(___padding, __COUNTER__)[128]
#endif

#define TM_ALLOC_FAA __sync_fetch_and_add
#define TM_ALLOC_BCAS __sync_bool_compare_and_swap
#define TM_ALLOC_PAUSE __asm__ __volatile__("pause;")

#ifndef SOFTWARE_BARRIER
#define SOFTWARE_BARRIER asm volatile("" : : : "memory")
#endif

#ifndef MAX_ALLOC_POW2_SIZE_CLASS
#define MAX_ALLOC_POW2_SIZE_CLASS 16  // 2^16 = 65536
#endif

#ifndef TM_ALLOC_PAGE_SIZE_BYTES
#define TM_ALLOC_PAGE_SIZE_BYTES 4 * 1024 * 1024ULL // 4MB
#endif

#define TM_ALLOC_USABLE_PAGE_SIZE_BYTES (TM_ALLOC_PAGE_SIZE_BYTES - sizeof(MemoryPage))

#ifndef MAX_THREADS_POW2
#define MAX_THREADS_POW2 256
#endif

// #define TM_ALLOC_BUFFER_SIZE 64
#define TM_ALLOC_BUFFER_SIZE 256

// this needs renaming - using -1 to indicate allocations that spanned more than 1 page
// these are special 'overflow' allocations where we allocate / free by multiple pages
// and we dont reset the pages meaning their free list is always empty
#define OVERFLOW_PAGE_SIZE_CLASS_POW2 -1

#define PAGE_MASK ~(TM_ALLOC_PAGE_SIZE_BYTES - 1)

#define PAGE_IS_FULL 1

using std::uint64_t;

class PMEMAlloc {
   private:
    typedef uintptr_t Block;

    PAD;

    struct MemoryPage {
        int64_t ownerTid;
        void* start;
        uint64_t sizeClassPow2;
        uint64_t sizeClassSize;
        PAD;
        volatile int sharedPageData_lock;
        volatile uint64_t isFull;
        MemoryPage* volatile next;
        MemoryPage* volatile prev;
        PAD;
        Block* freeList_head;
        volatile int crossThreadFreeList_lock;
        Block* crossThreadFreeList_head;
#ifdef ALLOC_DEBUG
        PAD;
        uint64_t numFreeObj;
#endif
        PAD;
    };

    PAD;
    struct ThreadData {
        PAD;
        MemoryPage* nonFullPageListsPerSizeClass[MAX_ALLOC_POW2_SIZE_CLASS];
        // PAD;

        MemoryPage* volatile fullPageList_head;
        volatile int fullPageList_lock;
        PAD;

        MemoryPage* volatile recheckPageListPerSizeClass[MAX_ALLOC_POW2_SIZE_CLASS];
        volatile int recheckPageList_lock;
        PAD;

        MemoryPage* volatile overflowPageList_head;
        volatile int overflowPageList_lock;
        PAD;

        // long garbage = 0;
#ifdef ALLOC_DEBUG
        PAD;
        uint32_t debugAllocCount = 0;
        uint32_t debugFreeCount = 0;
        uint32_t debugNumCommit = 0;
        uint32_t debugNumAbort = 0;
        PAD;
#endif
    };

    volatile uint64_t numAllocatedPages = 0;  // number of pages that have been allocated. This is incremented atomically
    PAD;

    MemoryPage* volatile emptyOverFlowPageList_head;
    volatile uint64_t emptyOverFlowPageList_size = 0;
    volatile int emptyOverFlowPageList_lock;
    PAD;

    uint8_t* baseAddr;
    uint64_t mmapSize;
    uint64_t totalPages;

    PAD;

    ThreadData tData[MAX_THREADS_POW2];
    PAD;

    inline uint64_t getSizeClass(uint64_t size) {
        uint64_t bitIndex = 0;
        while (size >> (bitIndex + 1)) {
            bitIndex++;
        }
        if (size > (1ULL << bitIndex)) {
            bitIndex++;
        }
        return bitIndex;
    }

    inline void acquireLock(volatile int* lock) {
        while (1) {
            if (*lock) {
                TM_ALLOC_PAUSE;
                continue;
            }
            if (TM_ALLOC_BCAS(lock, 0, 1)) {
                return;
            }
        }
    }

    inline void releaseLock(volatile int* lock) {
        SOFTWARE_BARRIER;
        *lock = 0;
    }

    void resetMemoryPage(MemoryPage* page) {                                         // this function also initializes a memory page
        uint64_t numBlocks = TM_ALLOC_USABLE_PAGE_SIZE_BYTES / page->sizeClassSize;  // sizeClassSize is set before calling this function
        page->crossThreadFreeList_head = nullptr;
        Block* obj = (Block*)page->start;  // start is set in initPages function, and points to where page metadata ends
        Block* nextObj = (Block*)(((uintptr_t)page->start) + page->sizeClassSize);
        page->freeList_head = obj;  // freeList_head is set to the start of the free memory
		assert(!page->freeList_head || (page->freeList_head && (*page->freeList_head || *page->freeList_head == 0)));
        for (uint64_t i = 0; i < numBlocks - 1; i++) {
            *((uintptr_t*)obj) = (uintptr_t)nextObj;  // set the next pointer to the next block

            obj = (Block*)(((uintptr_t)obj) + page->sizeClassSize);
            nextObj = (Block*)(((uintptr_t)nextObj) + page->sizeClassSize);
        }
        *((uintptr_t*)obj) = (uintptr_t)(nullptr);
    }

    void initThreads() {
        // loop through the threads
        for (int i = 0; i < MAX_THREADS_POW2; i++) {
            // We have size valid size classes from 0 to MAX_ALLOC_POW2_SIZE_CLASS (not including the max itself)
            // The actual sizes are 2^0, 2^1, 2^2, ... 2^(MAX_ALLOC_POW2_SIZE_CLASS-1) bytes
            // TODO: maybe optimize that to use our actual size classes?
            for (int szClass = 0; szClass < MAX_ALLOC_POW2_SIZE_CLASS; szClass++) {
                // every thread has a nonFullPageListsPerSizeClass
                tData[i].nonFullPageListsPerSizeClass[szClass] = nullptr;

                // every thread has a recheckPageListPerSizeClass
                tData[i].recheckPageListPerSizeClass[szClass] = nullptr;
            }

            tData[i].fullPageList_lock = 0;
            tData[i].recheckPageList_lock = 0;
            tData[i].fullPageList_head = nullptr;
        }
    }

    void initPages() {
        // let's say the page size is 4kb
        // totalPages has been set before calling this function
        // we divide the mmapped area into 4kb chunks, and initialize each chunk as a MemoryPage
        // the first bytes of each chunk will be the MemoryPage struct
        // the rest of the chunk will be up for allocation
        // the memorypage->start will point to the start of the free memory

        for (uint64_t i = 0; i < totalPages; i++) {
            MemoryPage* pageData = (MemoryPage*)(((uintptr_t)baseAddr) + i * TM_ALLOC_PAGE_SIZE_BYTES);
            pageData->ownerTid = -1;
            pageData->next = nullptr;
            pageData->prev = nullptr;
            pageData->isFull = 0;
            pageData->crossThreadFreeList_lock = 0;
            pageData->crossThreadFreeList_head = nullptr;
            pageData->start = (void*)(((uintptr_t)pageData) + sizeof(MemoryPage));
        }
    }

    inline void* tryAlloc(const int tid, uint64_t sizeClass) {
        ThreadData* td = &tData[tid];
        MemoryPage* page = td->nonFullPageListsPerSizeClass[sizeClass];

        if (!page) {
            // we dont have any non full pages
            return nullptr;
        }

        // do we have an object in our freelist?
        if (page->freeList_head) {
            Block* ptr = page->freeList_head;
            // SOFTWARE_BARRIER;
            page->freeList_head = (Block*)(*((uintptr_t*)(page->freeList_head)));  // the content of the freelist head ptr is the next block
            assert(((uint64_t)page->freeList_head) < ((uint64_t)page + (TM_ALLOC_PAGE_SIZE_BYTES)));
            assert(!page->freeList_head || (page->freeList_head && (*page->freeList_head || *page->freeList_head == 0)));

            return (void*)ptr;
        }

        if (page->crossThreadFreeList_head) {
            acquireLock(&(page->crossThreadFreeList_lock));
            // we only move the cross thread free list if our free list is empty
            //  so we dont need to do any appends and we dont need a tail ptr
            //  td->garbage += (*((uintptr_t*)(page->crossThreadFreeList_head)));
            page->freeList_head = page->crossThreadFreeList_head;
            assert(!page->freeList_head || (page->freeList_head && (*page->freeList_head || *page->freeList_head == 0)));
            // SOFTWARE_BARRIER;
            page->crossThreadFreeList_head = nullptr;
            // td->garbage += (*((uintptr_t*)(page->freeList_head)));
            releaseLock(&(page->crossThreadFreeList_lock));

            // we could have done this before moving the cross thread free list
            //  but its the same number of writes and we get to release the lock sooner this way
            Block* ptr = page->freeList_head;
            // freeList = freeList->next
            page->freeList_head = (Block*)(*((uintptr_t*)(page->freeList_head)));
            assert(!page->freeList_head || (page->freeList_head && (*page->freeList_head || *page->freeList_head == 0)));

            return (void*)ptr;
        }

        // this page is actually full it needs to be moved from the non-full to full lists
        //  returning nullptr in this case
        return nullptr;
    }

   public:
    PMEMAlloc() {
    }

    ~PMEMAlloc() {
    }

    void init() {
        initThreads();
        initPages();
    }

    uint8_t* aligned(uint8_t* addr) {
        // return (uint8_t*)((size_t)addr & (~0x3FULL)) + 128;
        return (uint8_t*)((size_t)addr & (PAGE_MASK)) + TM_ALLOC_PAGE_SIZE_BYTES;
    }

    void init(void* addressOfMemoryPool, size_t sizeOfMemoryPool) {
        baseAddr = aligned((uint8_t*)addressOfMemoryPool);
        mmapSize = sizeOfMemoryPool + (uint8_t*)(addressOfMemoryPool)-baseAddr;
        totalPages = mmapSize / (TM_ALLOC_PAGE_SIZE_BYTES);

        printf("PMEM Alloc Init:\n");

        printf("\n\nAligned base addr=%p\n\n", baseAddr);

        init();

        printf("\n\nINIT: PMEM alloc with baseAddr=%p and mmapSize=%ld, up to %p\n\n", baseAddr, mmapSize, (void*)((uint64_t)baseAddr + (uint64_t)mmapSize));
        printf("Page size: %llu\n", TM_ALLOC_PAGE_SIZE_BYTES);
        printf("Page count: %ld\n", totalPages);
        printf("Page mask: %p\n", (void*)PAGE_MASK);
        printf("Size of page meta data %ld\n", sizeof(MemoryPage));
        printf("Size of thread data %ld\n", sizeof(ThreadData));
        printf("\n\n");
    }

    void* getBaseAddr() {
        return this->baseAddr;
    }

    void* alloc(const int tid, size_t size) {
        ThreadData* td = &tData[tid];

        if (size <= TM_ALLOC_USABLE_PAGE_SIZE_BYTES) {  // if smaller than what fits in a page
            while (true) {
                uint64_t sizeClass = getSizeClass(size);  // get the size class
                void* ptr = tryAlloc(tid, sizeClass);
                if (ptr) {
                    return ptr;
                }

                // if we are here, either the head of nonFullPageList is full or our non-full page list is empty

                // our non full page list was full
                if (td->nonFullPageListsPerSizeClass[sizeClass]) {
                    // move the page to full page list and need to get a new one
                    MemoryPage* newFullPage = td->nonFullPageListsPerSizeClass[sizeClass];
                    uint64_t isFull = newFullPage->isFull;
                    assert(!(isFull & PAGE_IS_FULL));
                    assert(newFullPage == td->nonFullPageListsPerSizeClass[sizeClass]);

                    // advance to next non-full page
                    td->nonFullPageListsPerSizeClass[sizeClass] = td->nonFullPageListsPerSizeClass[sizeClass]->next;

                    // newFullPage->isFull = true;

                    assert(newFullPage->prev == nullptr);

                    acquireLock(&(td->fullPageList_lock));
                    isFull = newFullPage->isFull;

                    newFullPage->isFull++;

                    MemoryPage* oldFullPageListHead = td->fullPageList_head;
                    newFullPage->next = oldFullPageListHead;
                    if (oldFullPageListHead) {
                        oldFullPageListHead->prev = newFullPage;
                    }
                    td->fullPageList_head = newFullPage;
                    releaseLock(&(td->fullPageList_lock));

                    // check if we have another non-full page in the list
                    if (td->nonFullPageListsPerSizeClass[sizeClass]) {
                        td->nonFullPageListsPerSizeClass[sizeClass]->prev = nullptr;
                        continue;
                    }
                }

                // check if we have a page in the recheck list
                // if we do, move them to nonFull
                // we only get here if our nonFullPageList is empty
                if (td->recheckPageListPerSizeClass[sizeClass]) {
                    acquireLock(&(td->recheckPageList_lock));
                    td->nonFullPageListsPerSizeClass[sizeClass] = td->recheckPageListPerSizeClass[sizeClass];
                    td->recheckPageListPerSizeClass[sizeClass] = nullptr;
                    releaseLock(&(td->recheckPageList_lock));
                    continue;
                }

                // we have no pages or we have filled all of our threads pages so we need to get a new one
                uint64_t pageIndex = TM_ALLOC_FAA(&numAllocatedPages, 1);

                if (pageIndex >= totalPages) {
                    printf("ERROR: Out of memory\n");
                    exit(-1);
                }

                MemoryPage* page = (MemoryPage*)(((uintptr_t)baseAddr) + (TM_ALLOC_PAGE_SIZE_BYTES * pageIndex));
                page->ownerTid = tid;
                page->sizeClassPow2 = sizeClass;
                page->sizeClassSize = 1ULL << sizeClass;
                page->isFull = 0;
                SOFTWARE_BARRIER;
                resetMemoryPage(page);
                td->nonFullPageListsPerSizeClass[sizeClass] = page;
                // now the loop will try to allocate again, and this time it will likely succeed
            }

        } else {
            uint64_t requiredPages = (size + (TM_ALLOC_USABLE_PAGE_SIZE_BYTES - 1)) / TM_ALLOC_USABLE_PAGE_SIZE_BYTES;
            requiredPages += 1;
            MemoryPage* page = nullptr;

            // check for pages in global overflow list
            if (emptyOverFlowPageList_head) {
                acquireLock(&(emptyOverFlowPageList_lock));
                MemoryPage* currOvfPage = emptyOverFlowPageList_head;
                while (currOvfPage) {
                    if (currOvfPage->sizeClassSize <= size) {
                        page = currOvfPage;
                    }
                }
                releaseLock(&(td->overflowPageList_lock));

                if (page) {
                    return page->start;
                }
            }

            // get fresh pages
            uint64_t pageIndex = TM_ALLOC_FAA(&numAllocatedPages, requiredPages);

            if (pageIndex >= totalPages || pageIndex + requiredPages >= totalPages) {
                printf("ERROR WE ARE OUT OF MEMORY IN THIS MAPPING\nEXITING\n");
                exit(-1);
            }

            page = (MemoryPage*)(((uintptr_t)baseAddr) + (TM_ALLOC_PAGE_SIZE_BYTES * pageIndex));
            memset(page, 0, TM_ALLOC_PAGE_SIZE_BYTES * requiredPages);
            page->start = (void*)(((uintptr_t)page) + sizeof(MemoryPage));
            page->ownerTid = tid;
            page->sizeClassPow2 = OVERFLOW_PAGE_SIZE_CLASS_POW2;
            page->sizeClassSize = size;
            page->freeList_head = nullptr;
            page->isFull = 0;

            SOFTWARE_BARRIER;
            acquireLock(&(td->overflowPageList_lock));
            MemoryPage* oldHead = td->overflowPageList_head;
            page->next = oldHead;
            page->prev = nullptr;
            if (oldHead) {
                oldHead->prev = page;
            }
            td->overflowPageList_head = page;
            releaseLock(&(td->overflowPageList_lock));

            return page->start;
        }
    }

    void free(const int tid, void* ptr) {
        // get your own thread data
        ThreadData* td = &tData[tid];
        Block* obj = (Block*)ptr;

        // get the page that the ptr belongs to
        MemoryPage* page = (MemoryPage*)(((uintptr_t)obj) & PAGE_MASK);
        uint64_t pageSizeClass = page->sizeClassPow2;

        // if the page is not an overflow page
        if (pageSizeClass != OVERFLOW_PAGE_SIZE_CLASS_POW2) {
            // if I own the page
            if (page->ownerTid == tid) {
                // add the object to the free list (as the new head)
                *((uintptr_t*)obj) = (uintptr_t)(page->freeList_head); 
					assert(*obj || *obj == 0);

                page->freeList_head = obj;
                uint64_t isFull = page->isFull;
                
                if (isFull & PAGE_IS_FULL) {
                    if (!TM_ALLOC_BCAS(&page->isFull, isFull, isFull + 1)) {
                        return;
                    }

                    // the page is non-full now
                    acquireLock(&(td->fullPageList_lock));
                    if ((page->isFull & PAGE_IS_FULL)) {
                        releaseLock(&(td->fullPageList_lock));
                        exit(-1);  // should never get here we either CAS to not full or goto next obj
                        // goto NEXT_OBJ;
                    }
                    MemoryPage* pnext = page->next;
                    MemoryPage* pprev = page->prev;
                    if (pnext) {
                        pnext->prev = pprev;
                    }
                    if (pprev) {
                        pprev->next = pnext;
                    }
                    if (td->fullPageList_head == page) {
                        td->fullPageList_head = td->fullPageList_head->next;
                    }

                    // move the page to the nonfull page list
                    MemoryPage* oldHead = td->nonFullPageListsPerSizeClass[pageSizeClass];
                    // page->isFull = false;
                    page->prev = nullptr;
                    page->next = oldHead;
                    if (oldHead) {
                        oldHead->prev = page;
                    }
                    td->nonFullPageListsPerSizeClass[pageSizeClass] = page;
                    // we wait to release the lock on the full page list
                    // to guard the scenario where 2 threads are freeing from the same
                    // full page
                    releaseLock(&(td->fullPageList_lock));
                }

            } else {  // if I don't own the page
                      // get the page owners thread data
                ThreadData* ownerTd = &tData[page->ownerTid];

                // cross thread free
                acquireLock(&(page->crossThreadFreeList_lock));
                *((uintptr_t*)obj) = (uintptr_t)(page->crossThreadFreeList_head);
                assert(*obj || *obj == 0);
                page->crossThreadFreeList_head = obj;

                // td->garbage += *((uintptr_t*)(page->crossThreadFreeList_head));
                releaseLock(&(page->crossThreadFreeList_lock));

                uint64_t isFull = page->isFull;
                // check if page was full and move it if it was
                if (isFull & PAGE_IS_FULL) {
                    if (!TM_ALLOC_BCAS(&page->isFull, isFull, isFull + 1)) {
                        return;
                    } else {
                    }

                    // remove the page from the full page list
                    acquireLock(&(ownerTd->fullPageList_lock));
                    // need to recheck if the page is still full since
                    // another thread could have concurrently moved it
                    if ((page->isFull & PAGE_IS_FULL)) {
                        releaseLock(&(ownerTd->fullPageList_lock));
                        exit(-1);  // should never get here we either CAS to not full or goto next obj
                        // goto NEXT_OBJ;
                    }

                    MemoryPage* next = page->next;
                    MemoryPage* prev = page->prev;
                    if (next) {
                        assert(next);
                        page->next->prev = prev;
                    }
                    if (prev) {
                        assert(prev);
                        page->prev->next = next;
                    }
                    if (ownerTd->fullPageList_head == page) {
                        ownerTd->fullPageList_head = ownerTd->fullPageList_head->next;
                    }

                    // move page to the recheck page list
                    //  need to lock since we dont own the list
                    acquireLock(&(ownerTd->recheckPageList_lock));
                    MemoryPage* oldHead = ownerTd->recheckPageListPerSizeClass[pageSizeClass];
                    // page->isFull = false;
                    page->prev = nullptr;
                    page->next = oldHead;
                    if (oldHead) {
                        oldHead->prev = page;
                    }
                    ownerTd->recheckPageListPerSizeClass[pageSizeClass] = page;
                    releaseLock(&(ownerTd->recheckPageList_lock));
                    // we wait to release the lock on the full page list
                    // to guard the scenario where 2 threads are freeing from the same
                    // full page
                    releaseLock(&(ownerTd->fullPageList_lock));
                }
            }
        } else {  // if the page IS an overflow page
            ThreadData* ownerTd = &tData[page->ownerTid];
            acquireLock(&(ownerTd->overflowPageList_lock));
            acquireLock(&(emptyOverFlowPageList_lock));
            MemoryPage* next = page->next;
            MemoryPage* prev = page->prev;
            if (next) {
                page->next->prev = prev;
            }
            if (prev) {
                page->prev->next = next;
            }
            if (ownerTd->overflowPageList_head == page) {
                ownerTd->overflowPageList_head = ownerTd->overflowPageList_head->next;
            }

            // GUY::NOTES:: removed the below since anyone who ends up
            //	doing another overflow allocation will need the pages to be contiguous
            //	so adding individual pages wont help unless we want to carve them up
            //	again when we overflow allocate which seems bad
            //  	---
            // add the individual pages that make up the overflow allocation
            //  since it spans more than 1
            //  uint64_t overflowPageCount = (page->sizeClassSize + (TM_ALLOC_USABLE_PAGE_SIZE_BYTES - 1)) / TM_ALLOC_USABLE_PAGE_SIZE_BYTES;
            //  for (int ofpcIdx = 0; ofpcIdx < overflowPageCount; ofpcIdx++) {
            //  	MemoryPage* ovPage = (MemoryPage*)(((uintptr_t)page) + (TM_ALLOC_PAGE_SIZE_BYTES * ofpcIdx));
            //  	ovPage->prev = nullptr;
            //  	emptyOverFlowPageList_head->prev = ovPage;
            //  	ovPage->next = emptyOverFlowPageList_head;
            //  	emptyOverFlowPageList_head = ovPage;
            //  }
            //  emptyOverFlowPageList_size += overflowPageCount;
            emptyOverFlowPageList_size++;
            MemoryPage* emptyOvfHead = emptyOverFlowPageList_head;
            page->next = emptyOvfHead;
            emptyOvfHead->prev = page;
            emptyOverFlowPageList_head = page;

            releaseLock(&(emptyOverFlowPageList_lock));
            releaseLock(&(ownerTd->overflowPageList_lock));
        }
    }
};

#endif