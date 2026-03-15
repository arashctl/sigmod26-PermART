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
#include "zipfian.h"

#define COUTATOMIC(coutstr) /*cout<<coutstr*/ \
{ \
    std::stringstream ss; \
    ss<<coutstr; \
    std::cout<<ss.str(); \
}

int main(int argc, char * argv[]) { 
    float zipfianParam = 0.99;

    if (argc > 1) {
        zipfianParam = atof(argv[1]);
    }

    // COUTATOMIC("Zipfian parameter: " << zipfianParam << "\n"); 


    using KeyType = unsigned long; 
    KeyType maxKey = ~0ul;


    const int dsSize = 10000000; 
    const int numUniqueKeys = dsSize * 2;
    const int threadNum = 144; 

    auto x = parlay::delayed_tabulate(1.2 * numUniqueKeys, [&] (size_t i) {
        return (KeyType) parlay::hash64(i);
    });

    auto uniqueKeys = parlay::remove_duplicates(x); 
    uniqueKeys = parlay::random_shuffle(uniqueKeys);

    uniqueKeys = parlay::tabulate(numUniqueKeys, [&] (size_t i) {
        return 1 + std::min(maxKey - 1, uniqueKeys[i]);
    });

    parlay::parallel_for(0, numUniqueKeys, [&] (size_t i) {
       COUTATOMIC("u " << uniqueKeys[i] << "\n");
    });

    Zipfian z(numUniqueKeys, zipfianParam);

    const int generateSize = 10 * dsSize + 1000 * threadNum;
    uniqueKeys = parlay::tabulate(generateSize, [&] (size_t i) {
        return uniqueKeys[z(i)];
    });

    auto generatedKeys = parlay::random_shuffle(uniqueKeys);

    parlay::parallel_for(0, 200000, [&] (size_t i) {
    //    COUTATOMIC("uniqueKeys[" << i << "] = " << uniqueKeys[i] << "\n");
        COUTATOMIC(generatedKeys[i] << "\n");
    });


    return 0;
}