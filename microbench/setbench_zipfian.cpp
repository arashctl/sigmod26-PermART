


#include <iostream> 
#include <atomic>
#include <sstream>
#include <algorithm>
#include "omp.h"
#include <cstring>

#include "define_global_statistics.h"
#include "gstats_global.h"  // include the GSTATS code and macros (crucial this happens after GSTATS_HANDLE_STATS is defined)


#include "globals_extern.h"

#include "random_xoshiro256p.h"
#include "keygen.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <ctime>
#include <limits>
#include <omp.h>
#include <parallel/algorithm>
#include <perftools.h>
#include <set>
#include <thread>
#include <algorithm>
#include <random>

#define COUTATOMIC(coutstr) /*cout<<coutstr*/ \
{ \
    std::stringstream ss; \
    ss<<coutstr; \
    std::cout<<ss.str(); \
}

using test_type = long long;

int main() { 
    Random64 rng; 
    srand(time(0)); 
    rng.setSeed(rand());

    double zipfianParam = 0.99;
    int maxKey = 20000000;

    auto keygenzipfdata = new KeyGeneratorZipfData(maxKey, zipfianParam);
    auto keygen = new KeyGeneratorZipf<test_type>(keygenzipfdata, &rng);

    for (int i = 0; i < 200000; ++i) { 
        test_type key = keygen->next();
        std::cout << key << std::endl;
    }

}



// #include <algorithm>
// #include <math.h>

// class KeyGeneratorZipfData {
// public:
//     int maxKey;
//     double c = 0; // Normalization constant
//     double* sum_probs; // Pre-calculated sum of probabilities
//     KeyGeneratorZipfData(const int _maxKey, const double _alpha) {
//         maxKey = _maxKey;
//         // Compute normalization constant c for implied key range: [1, maxKey]
//         for (int i = 1; i <= _maxKey; i++) {
//             c += ((double)1) / pow((double) i, _alpha);
//         }
//         double* probs = new double[_maxKey+1];
//         for (int i = 1; i <= _maxKey; i++) {
//             probs[i] = (((double)1) / pow((double) i, _alpha)) / c;
//         }
//         // Random should be seeded already (in main)
//         std::random_shuffle(probs + 1, probs + maxKey);
//         sum_probs = new double[_maxKey+1];
//         sum_probs[0] = 0;
//         for (int i = 1; i <= _maxKey; i++) {
//             sum_probs[i] = sum_probs[i - 1] + probs[i];
//         }

//         delete[] probs;
//     }
//     ~KeyGeneratorZipfData() {
//         delete[] sum_probs;
//     }
// };

// template <typename K>
// class KeyGeneratorZipf {
// private:
//     KeyGeneratorZipfData * data;
//     Random64 * rng;
// public:
//     KeyGeneratorZipf(KeyGeneratorZipfData * _data, Random64 * _rng)
//           : data(_data), rng(_rng) {}
//     K next() {
//         double z; // Uniform random number (0 < z < 1)
//         int zipf_value = 0; // Computed exponential value to be returned
//         // Pull a uniform random number (0 < z < 1)
//         do {
//             z = (rng->next() / (double) std::numeric_limits<uint64_t>::max());
//         } while ((z == 0) || (z == 1));
//         zipf_value = std::upper_bound(data->sum_probs + 1, data->sum_probs + data->maxKey + 1, z) - data->sum_probs;
//         // Assert that zipf_value is between 1 and N
//         assert((zipf_value >= 1) && (zipf_value <= data->maxKey));
//         // GSTATS_ADD_IX(tid, key_gen_histogram, 1, zipf_value);
//         return (zipf_value);
//     }
// };