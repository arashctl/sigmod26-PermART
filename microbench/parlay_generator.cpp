#include <algorithm>
#include <artifact_parlay/internal/get_time.h>
#include <artifact_parlay/internal/group_by.h>
#include <artifact_parlay/io.h>
#include <artifact_parlay/primitives.h>
#include <artifact_parlay/random.h>
#include <atomic>
#include <iostream>
#include <random>
#include <set>
#include <sstream>
#include <thread>

#include "omp.h"

// g++-10 parlay_generator.cpp -std=c++17 `find ../common -type d | sed s/^/-I/` -fopenmp -lpthread


// GOAL: find a way to do the job they do, but without parlay:: utilities.
// MAKE it as fast.

typedef unsigned long test_type;

#define COUTATOMIC(coutstr) /*cout<<coutstr*/ \
    {                                         \
        std::stringstream ss;                 \
        ss << coutstr;                        \
        std::cout << ss.str();                \
    }

int main() {
    int numUniqueKeys = 2000000;

    test_type _maxKey = (~0ul) >> 0;

    std::vector<test_type> preliminary;

    preliminary.resize(numUniqueKeys * 1.2);

#pragma omp parallel for
    for (int i = 0; i < preliminary.size(); i++) {
        preliminary[i] = (test_type)parlay::hash64(i) & _maxKey;  // 64-bit keys (0x000...00 to 0xFFF...FF)
    }

    COUTATOMIC("Generated " << preliminary.size() << " keys\n");
    COUTATOMIC("Sorting...\n");
    std::sort(preliminary.begin(), preliminary.end());

    COUTATOMIC("Removing duplicates...\n");
    preliminary.erase(std::unique(preliminary.begin(), preliminary.end()), preliminary.end());

    COUTATOMIC("Final size: " << preliminary.size() << "\n");

    auto rng = std::default_random_engine{};
    COUTATOMIC("Shuffling...\n");
    std::shuffle(preliminary.begin(), preliminary.end(), rng);

    COUTATOMIC("Is preliminary unique?\n"); 
    std::set<test_type> preliminarySet;
    for (auto key : preliminary) {
        if (preliminarySet.find(key) != preliminarySet.end()) {
            COUTATOMIC("NO\n");
            COUTATOMIC("Duplicate key: " << key << "\n");
            return 1;
        }
        preliminarySet.insert(key);
    }
    COUTATOMIC("YES\n");


    std::vector<test_type> uniqueKeys;
    uniqueKeys.resize(numUniqueKeys);
#pragma omp parallel for
    for (int i = 0; i < numUniqueKeys; i++) {
        uniqueKeys[i] = 1 + std::min(_maxKey - 1, preliminary[i]);
    }

    COUTATOMIC("Generated " << uniqueKeys.size() << " unique keys\n");

    COUTATOMIC("Is it unique?\n"); 

    std::set<test_type> uniqueSet;
    
    for (auto key : uniqueKeys) {
        if (uniqueSet.find(key) != uniqueSet.end()) {
            COUTATOMIC("NO\n");
            COUTATOMIC("Duplicate key: " << key << "\n");
            return 1;
        }
        uniqueSet.insert(key);
    }

    COUTATOMIC("YES\n");

    return 0;
}
