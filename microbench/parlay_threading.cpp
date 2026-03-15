#include <iostream> 
#include <atomic>
#include <sstream>
#include <algorithm>
#include <thread>
#include "omp.h"
#include <artifact_parlay/primitives.h>
#include <artifact_parlay/random.h>
#include <artifact_parlay/io.h>
#include <artifact_parlay/internal/get_time.h>
#include <artifact_parlay/internal/group_by.h>



#define COUTATOMIC(coutstr) /*cout<<coutstr*/ \
{ \
    std::stringstream ss; \
    ss<<coutstr; \
    std::cout<<ss.str(); \
}


#ifndef THREAD_COUNT
#define THREAD_COUNT 16
#endif


int calc(int x) { 
    int res = 0;
    for (int i = 0; i < x * x * x * x; i++) {
        res += i % x;
    }
    return res;
}

void thread_func(int tid) {
    int parlay_tid = parlay::my_thread_id();
    COUTATOMIC("tid: " << tid << " parlay_tid: " << parlay_tid << "\n");
    auto x = calc(tid);
    COUTATOMIC("x: " << x << "\n");
}

int main() { 
    std::thread *theads[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        theads[i] = new std::thread(thread_func, i);
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
        theads[i]->join();
        // delete theads[i];
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
        theads[i] = new std::thread(thread_func, i);
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
        theads[i]->join();
        delete theads[i];
    }


    return 0; 
}

// int main() { 
//     std::atomic<int> x = 0;



//     parlay::parallel_for(0, 10, [&] (size_t i) {
//         auto tid = parlay::my_thread_id();
//         // making it heavy so that the threads are not done too quickly, and ten threads are created
//         for (int i = 0; i < 1000; i++) {
//             x.fetch_add(i);
//         }
//     });

//     // If I do a omp parallel for and call parlay::my_thread_id in it, the first thread ids are reserved for omp threads, and are never reused
//     // that causes actual parlay threads to get thread ids starting form 10, which causes segfaults later on 
//     // fix: reserve thread ids 0 to 512 for parlay by calling parlay::my_thread_id in a parlay parallel_for before any omp parallel for

//     COUTATOMIC("Before omp parallel for\n");


//     #pragma omp parallel for
//     for (int i = 0; i < 10; i++) {

//         auto tid = parlay::my_thread_id();
//         auto omp_tid = omp_get_thread_num();
//         // YES
//         // the problem happens when a non-parlay thread tries to know its own parlay::thread_id 
//         // otherwise it works fine 
//         x.fetch_add(i);
//     }

//     COUTATOMIC("Before first parallel_for\n");
//     parlay::parallel_for(0, 10, [&] (size_t i) {
//         auto tid = parlay::my_thread_id();
//         COUTATOMIC("tid: " << tid << "\n");
//         // making it heavy so that the threads are not done too quickly, and ten threads are created
        
//         x.fetch_add(i);
//     });



//     COUTATOMIC("Before second parallel for\n");
//     parlay::parallel_for(0, 10, [&] (size_t i) {
//         auto tid = parlay::my_thread_id();
//         COUTATOMIC("tid: " << tid << "\n");

//         x.fetch_add(i);
//     });

    

//     COUTATOMIC("Before third parallel for\n");
//     parlay::parallel_for(0, 10, [&] (size_t i) {
//         auto tid = parlay::my_thread_id();
//         COUTATOMIC("tid: " << tid << "\n");

//         x.fetch_add(i);
//     });

//     COUTATOMIC("x: " << x << "\n");
//     return 0;
// }