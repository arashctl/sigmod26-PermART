#include <iostream> 
#include <atomic>
#include <sstream>
#include <algorithm>
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



int main() { 

    std::atomic<int> x = 0;

    for (int k = 0; k < 10; k++) {
parlay::parallel_for(0, 10, [&] (size_t i) {
        auto tid = parlay::my_thread_id();
        COUTATOMIC("tid: " << tid << "\n");
        // making it heavy so that the threads are not done too quickly, and ten threads are created
        for (int i = 0; i < 1000; i++) {
            x.fetch_add(i);
        }
    });
    }
    
    COUTATOMIC("Now omp for\n");

    // omp for 
    #pragma omp parallel for
    for (int i = 0; i < 10; i++) {

        auto tid = parlay::my_thread_id();
        auto omp_tid = omp_get_thread_num();
        // YES
        // the problem happens when a non-parlay thread tries to know its own parlay::thread_id 
        // otherwise it works fine 
        x.fetch_add(i);
    }


    COUTATOMIC("Before last parallel_for\n");
    parlay::parallel_for(0, 10, [&] (size_t i) {
        auto tid = parlay::my_thread_id();
        COUTATOMIC("tid: " << tid << "\n");

        // making it heavy so that the threads are not done too quickly, and ten threads are created
        for (int i = 0; i < 1000; i++) {
            x.fetch_add(i);
        }
    });

    std::cout << x << std::endl;

    return 0;
}