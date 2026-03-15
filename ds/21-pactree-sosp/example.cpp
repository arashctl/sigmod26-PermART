#include <iostream>

#include "pactree.h"
// #include "ralloc_n0log.hpp"
// #include "ralloc_n0sl.hpp"
// #include "ralloc_n0dl.hpp"
// #include "ralloc_n1log.hpp"
// #include "ralloc_n1sl.hpp"
// #include "ralloc_n1dl.hpp"

int main() {
    std::cout << "Hello, World!" << std::endl;

    #ifdef USE_PMDK
    std::cout << "Using PMDK allocator\n";
    #elif defined(USE_RALLOC)
    std::cout << "Using RALLOC allocator\n";
    #endif 

    pactree *pt = new pactree(2);
    pt->registerThread();

    int NUMKEYS = 1000;

    std::cout << "Inserting " << NUMKEYS << " keys\n";
    for (int i = 1; i < NUMKEYS; i++) {
        auto retval = pt->insert(i, i);
        assert(retval == true);
    }

    std::cout << "Re-inserting " << (NUMKEYS*2) << " keys\n";
    for (int i = 1; i < NUMKEYS*2; i++) {
        auto retval = pt->insert(i, i);

        assert(retval == (i >= NUMKEYS) );
    }

    std::cout << "Removing even keys\n";
    for (int i = 2; i < NUMKEYS*2; i += 2) {
        auto retval = pt->remove(i);
        assert(retval == true);
    }

    std::cout << "Looking up all keys\n";
    for (int i = 1; i < NUMKEYS*2; i++) {
        auto val = pt->lookup(i);

        // I'm expecting odd keys to be found 
        if (i % 2 == 1) {
            assert(val == i);
        } else {
            // assert(val != i);
        }

    }

    std::cout << "Doneeeeee" << std::endl;
    std::cout << std::flush;



    // RP_N0LOG_init("N0LOG", 25ULL * 1024 * 1024 * 1024);
    // RP_N0DL_init("N0DL", 25ULL * 1024 * 1024 * 1024);
    // RP_N0SL_init("N0SL", 25ULL * 1024 * 1024 * 1024);
    // RP_N1LOG_init("N1LOG", 25ULL * 1024 * 1024 * 1024);
    // RP_N1DL_init("N1DL", 25ULL * 1024 * 1024 * 1024);
    // RP_N1SL_init("N1SL", 25ULL * 1024 * 1024 * 1024);

    // auto x = RP_N0DL_malloc(64);
    // std::cout << "Allocated 64 bytes at " << x << "\n";
    // auto y = RP_N1DL_malloc(128);
    // std::cout << "Allocated 128 bytes at " << y << "\n";

    // if (RP_N0DL_in_prange(x)) {
    //     std::cout << x << " is in N0DL range.\n";
    // } else {
    //     std::cout << x << " is NOT in N0DL range.\n";
    // }

    // if (RP_N1DL_in_prange(x)) {
    //     std::cout << x << " is in N1DL range.\n";
    // } else {
    //     std::cout << x << " is NOT in N1DL range.\n";
    // }

    // if (RP_N0DL_in_prange(y)) {
    //     std::cout << y << " is in N0DL range.\n";
    // } else {
    //     std::cout << y << " is NOT in N0DL range.\n";
    // }

    // if (RP_N1DL_in_prange(y)) {
    //     std::cout << y << " is in N1DL range.\n";
    // } else {
    //     std::cout << y << " is NOT in N1DL range.\n";
    // }

    return 0;
}