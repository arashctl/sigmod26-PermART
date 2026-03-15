/*
 * File:   keygen.h
 * Author: t35brown
 *
 * Created on August 5, 2019, 7:24 PM
 */

#ifndef KEYGEN_H
#define KEYGEN_H

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <tuple>
#include <unordered_set>
#define MultiCoreUniqueKeyGen 1 
#ifdef MultiCoreUniqueKeyGen
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_sort.h>
#endif

#include "plaf.h"

#ifdef PregenerateOpTypes
enum OpType : char { FindOp, InsertOp, DeleteOp, RangeOp };
#endif

#ifdef MultiCoreUniqueKeyGen 
#define UniqueKeyGenThreadCount 90

template <typename K>
std::tuple<K *, K *, K, K> generateUniqueKeys(int n, Random64 *rng) {
    const double stop_coefficient =
        2;  // 1.33*n is the maximum number of iterations needed to generate
               // n unique keys
    
    tbb::concurrent_unordered_set<K> uniqueKeys;

    std::cout << "Generating " << n << " unique keys using "
              << UniqueKeyGenThreadCount << " threads\n";
    std::cout << std::flush;

    #pragma omp parallel for num_threads(UniqueKeyGenThreadCount)
    for (int t = 0; t < UniqueKeyGenThreadCount; ++t) {
        auto threadRng = rng[t]; 
        int total_generated = 0;

        while (uniqueKeys.size() < n) {
            K key = 0;
            do {
                key = threadRng.next();
            } while (key == 0);  // because +1 might cause overflow

            uniqueKeys.insert(key);
            total_generated++;

            if (total_generated >= (int)(stop_coefficient * n / UniqueKeyGenThreadCount)) {
                break; 
            }
        }

    }

    std::cout << "Generated " << uniqueKeys.size() << " unique keys\n";
    std::cout << std::flush;


    K *result = new K[n];
    K *sortedResult = new K[n];
    K maxKey = 0;
    K minKey = std::numeric_limits<K>::max();
    assert((int)uniqueKeys.size() >= n);
    auto it = uniqueKeys.begin();
    for (int i = 0; i < n; i++, it++) {
        K key = *it;
        assert(key != 0);
        if (key > maxKey) { maxKey = key; }
        if (key < minKey) { minKey = key; }
        result[i] = key;
        sortedResult[i] = key;
    }

    std::cout << "Min key: " << minKey << ", Max key: " << maxKey << "\n";
    std::cout << std::flush;

    tbb::parallel_sort(sortedResult, sortedResult + n);

    std::cout << "Sorting done\n";
    std::cout << "Sorted min key: " << sortedResult[0]
              << ", Sorted max key: " << sortedResult[n - 1] << "\n";
    std::cout << std::flush;

    return std::make_tuple(result, sortedResult, minKey, maxKey);

    
}


#else 

template <typename K>
std::tuple<K *, K *, K, K> generateUniqueKeys(int n, Random64 *rng) {
    const double stop_coefficient =
        1.33;  // 1.33*n is the maximum number of iterations needed to generate
               // n unique keys

    std::unordered_set<K> uniqueKeys;
    int total_generated = 0;

    while (uniqueKeys.size() < n) {
        K key = 0;
        do {
            key = rng->next();
        } while (key == 0);  // because +1 might cause overflow

        uniqueKeys.insert(key);
        total_generated++;

        if (total_generated >= (int)(stop_coefficient * n)) {
            std::cout
                << "Error: Could not generate enough unique keys. Exiting."
                << std::endl;
            exit(-1);
        }
    }

    K *result = new K[n];
    K *sortedResult = new K[n];
    K maxKey = 0;
    K minKey = std::numeric_limits<K>::max();

    auto it = uniqueKeys.begin();
    for (int i = 0; i < n; i++, it++) {
        K key = *it;
        assert(key != 0);

        if (key > maxKey) { maxKey = key; }
        if (key < minKey) { minKey = key; }

        result[i] = key;
        sortedResult[i] = key;
    }

    std::sort(sortedResult, sortedResult + n);
    return std::make_tuple(result, sortedResult, minKey, maxKey);
}
#endif 

template <typename K>
K *generateDenseUniqueKeys(int n) {
    K *result = new K[n];
#pragma omp parallel for
    for (int i = 0; i < n; i++) { result[i] = i + 1; }
    std::random_shuffle(result, result + n);
    return result;
}

// Uniform generator
// If dense, returns a random number in the range [1, n]
// If sparse returns a random number from n unique keys
template <typename K, bool is_sparse>
class KeyGeneratorUniform {
   private:
    PAD;
    Random64 *rng;
    int n;
    K *uniqueKeys;
    PAD;
    K *sortedUniqueKeys;  // only used for range queries in sparse mode
    PAD;

   public:
    KeyGeneratorUniform(Random64 *_rng, int _n, double unusedZipfParam,
                        void *_uniqueKeys, void *_sortedUniqueKeys, void *_unused)
        : rng(_rng), n(_n), uniqueKeys((K *)_uniqueKeys), sortedUniqueKeys((K *)_sortedUniqueKeys) {
    }

    K next() {
        if constexpr (is_sparse) {
            auto result = uniqueKeys[rng->next(n)];
            assert((result >= 1));
            return result;
        } else {
            auto result = 1 + rng->next(n);
            assert((result >= 1) && (result <= n));
            return result;
        }
    }

    std::pair<K, K> nextRange(K rangeSize, bool respectSkew = false) {
        // respectSkew is ignored for uniform distribution
        if constexpr (is_sparse) {
            int startIndex = rng->next(n - rangeSize + 1);
            return std::make_pair(sortedUniqueKeys[startIndex],
                                  sortedUniqueKeys[startIndex + rangeSize - 1]);
        } else {
            int start = rng->next(n - rangeSize + 1);
            return std::make_pair(1 + start, 1 + start + rangeSize - 1);
        }
    }
};

// Zipf key generator data
class KeyGeneratorZipfData {
   public:
    PAD;
    int maxKey;         // can mean numKeys in case of sparse key generation
    double c = 0;       // Normalization constant
    double *sum_probs;  // Pre-calculated sum of probabilities
    PAD;

    KeyGeneratorZipfData(const int _maxKey, const double _alpha)
        : maxKey(_maxKey) {
        // Compute normalization constant c for implied key range: [1, maxKey]
        for (int i = 1; i <= _maxKey; i++) {
            c += ((double)1) / pow((double)i, _alpha);
        }
        double *probs = new double[_maxKey + 1];
#pragma omp parallel for
        for (int i = 1; i <= _maxKey; i++) {
            probs[i] = (((double)1) / pow((double)i, _alpha)) / c;
        }
        // Random should be seeded already (in main)
        std::random_shuffle(probs + 1, probs + maxKey);
        sum_probs = new double[_maxKey + 1];
        sum_probs[0] = 0;
        for (int i = 1; i <= _maxKey; i++) {
            sum_probs[i] = sum_probs[i - 1] + probs[i];
        }

        delete[] probs;
    }

    ~KeyGeneratorZipfData() { delete[] sum_probs; }
};

template <typename K, bool is_sparse>
class KeyGeneratorZipf {
   private:
    PAD;
    KeyGeneratorZipfData *data;
    Random64 *rng;
    K *uniqueKeys;
    PAD;

   public:
    KeyGeneratorZipf(Random64 *_rng, int _maxKey, double _zipfParam,
                     void *_uniqueKeys, void *_data)
        : rng(_rng),
          data((KeyGeneratorZipfData *)_data),
          uniqueKeys((K *)_uniqueKeys) {
        // The zipf param is in KeyGeneratorZipfData
    }

    K next() {
        double z;            // Uniform random number (0 < z < 1)
        int zipf_value = 0;  // Computed exponential value to be returned
        // Pull a uniform random number (0 < z < 1)
        do { z = rng->nextDouble(); } while ((z == 0) || (z == 1));
        zipf_value = std::upper_bound(data->sum_probs + 1,
                                      data->sum_probs + data->maxKey + 1, z) -
                     data->sum_probs;
        // Assert that zipf_value is between 1 and N
        assert((zipf_value >= 1) && (zipf_value <= data->maxKey));
        // GSTATS_ADD_IX(tid, key_gen_histogram, 1, zipf_value);

        if constexpr (is_sparse) {
            return uniqueKeys[(zipf_value - 1)];
        } else {
            return zipf_value;
        }
    }
};

#ifdef ParlayDistributions
template <typename K>
class PrefillingParlayGenerator {
   private:
    PAD;
    Random64 *rng;
    std::vector<K> uniqueKeys;
    PAD;

   public:
    PrefillingParlayGenerator(Random64 *_rng, parlay::sequence<K> _uniqueKeys)
        : rng(_rng) {
        uniqueKeys.resize(_uniqueKeys.size());
        for (int j = 0; j < _uniqueKeys.size(); ++j) {
            uniqueKeys[j] = _uniqueKeys[j];
        }
    }

    K next() {
        auto ret = uniqueKeys[(rng->next() % uniqueKeys.size())];
        return ret;
    }
};

template <typename K>
class ParlayGenerator {
   public:
    PAD;
// K *keys;
#ifndef PregenerateOpTypes
    std::vector<K> keys;
#else
    // std::vector<std::pair<K, OpType>> keys;
    std::vector<K> keys;
    std::vector<OpType> opTypes;
#endif
    int i;
    int numSamples;
    PAD;

   public:
#ifndef BoycottParlay
    ParlayGenerator(parlay::sequence<K> _keys, int start, int end)
        : i(-1), numSamples(end - start) {
        keys.resize(numSamples);

        for (int j = start; j < end; ++j) { keys[j - start] = _keys[j]; }
    }

    K next() {
        i = (i + 1) % numSamples;
        return keys[i];
    }
#else
    ParlayGenerator(std::vector<K> &uniqueKeys, int start, int end, int m,
                    int insFrac, int delFrac, Zipfian *zipfian)
        : i(-1) {
        // std::string text = "ParlayGenerator: start=" + std::to_string(start)
        // + " end=" + std::to_string(end) + "\n"; std::cout << text;

        numSamples = end - start;
        // keys = new K[numSamples];
        keys.resize(numSamples);

#ifdef PregenerateOpTypes
        opTypes.resize(numSamples);
#endif

#ifndef PregenerateOpTypes
        for (int j = start; j < end; ++j) {
            if (zipfian == nullptr) {
                // uniform gen
                keys[j - start] =
                    uniqueKeys[parlay::hash64(j) % uniqueKeys.size()];
            } else {
                // zipfian gen
                keys[j - start] = uniqueKeys[(*zipfian)(j)];
            }
        }
#else
        for (int j = start; j < end; ++j) {
            OpType curOpType = OpType::FindOp;

            auto h = parlay::hash64(m + j) % 1000;
            if (h < 10 * insFrac) {
                curOpType = OpType::InsertOp;
            } else if (h < 10 * (insFrac + delFrac)) {
                curOpType = OpType::DeleteOp;
            } else {
                curOpType = OpType::FindOp;
            }

            if (zipfian == nullptr) {
                // uniform gen
                // keys[j - start] = std::make_pair(uniqueKeys[parlay::hash64(j)
                // % uniqueKeys.size()], curOpType);
                keys[j - start] =
                    uniqueKeys[parlay::hash64(j) % uniqueKeys.size()];
                opTypes[j - start] = curOpType;
            } else {
                // zipfian gen
                // keys[j - start] = std::make_pair(uniqueKeys[(*zipfian)(j)],
                // curOpType);
                keys[j - start] = uniqueKeys[(*zipfian)(j)];
                opTypes[j - start] = curOpType;
            }
        }

#endif
    }

#ifndef PregenerateOpTypes
    K next() {
        i = (i + 1) % numSamples;
        return keys[i];
    }

#else

    K next() {
        i = (i + 1) % numSamples;
        return keys[i];
    }

    OpType nextOpType() {
        // IMPORTANT: This function should only be called after next() has been
        // called
        return opTypes[i];
    }

#endif

#endif
};

#endif

// Sampler taken from
// https://commons.apache.org/proper/commons-math/apidocs/src-html/org/apache/commons/math4/distribution/ZipfDistribution.html#line.44
// Paper: Rejection-Inversion to Generate Variates from Monotone Discrete
// Distributions.
struct ZipfRejectionInversionSamplerData {
    int *mapping;
    const int maxkey;
    ZipfRejectionInversionSamplerData(int _maxkey) : maxkey(_maxkey) {
        mapping = new int[maxkey + 1];
#pragma omp parallel for
        for (int i = 0; i < maxkey + 1; ++i) { mapping[i] = i; }
        std::random_shuffle(mapping + 1, mapping + maxkey);
    }

    ~ZipfRejectionInversionSamplerData() { delete[] mapping; }
};

template <typename K, bool is_sparse>
class ZipfRejectionInversionSampler {
    const double exponent;
    const int maxkey;
    Random64 *rng;
    ZipfRejectionInversionSamplerData *const data;
    K *uniqueKeys;
    double hIntegralX1;
    double hIntegralmaxkey;
    double s;

    double hIntegral(const double x) {
        return helper2((1 - exponent) * log(x)) * log(x);
    }

    double h(const double x) { return exp(-exponent * log(x)); }

    double hIntegralInverse(const double x) {
        double t = x * (1 - exponent);
        if (t < -1) {
            // Limit value to the range [-1, +inf).
            // t could be smaller than -1 in some rare cases due to numerical
            // errors.
            t = -1;
        }
        return exp(helper1(t) * x);
    }

    double helper1(const double x) {
        // if (abs(x)>1e-8) {
        return log(x + 1) / x;
        // }
        // else {
        //     return 1.-x*((1./2.)-x*((1./3.)-x*(1./4.)));
        // }
    }

    double helper2(const double x) {
        // if (FastMath.abs(x)>1e-8) {
        return (exp(x) - 1) / x;
        // }
        // else {
        //     return 1.+x*(1./2.)*(1.+x*(1./3.)*(1.+x*(1./4.)));
        // }
    }

   public:
    /** Simple constructor.
     * @param maxkey number of elements
     * @param exponent exponent parameter of the distribution
     */
    ZipfRejectionInversionSampler(Random64 *_rng, int _maxKey,
                                  double _zipfParam, void *_uniqueKeys,
                                  void *_data)
        : rng(_rng),
          data((ZipfRejectionInversionSamplerData *)_data),
          uniqueKeys((K *)_uniqueKeys),
          maxkey(_maxKey),
          exponent(_zipfParam) {
        if (exponent <= 1) {
            std::cout
                << "-dist-zipf-fast only works with exponents greater than 1."
                << std::endl;
            exit(-1);
        }
        hIntegralX1 = hIntegral(1.5) - 1;
        hIntegralmaxkey = hIntegral(maxkey + 0.5);
        s = 2 - hIntegralInverse(hIntegral(2.5) - h(2));
    }

    /** Generate one integral number in the range [1, maxkey].
     * @param random random generator to use
     * @return generated integral number in the range [1, maxkey]
     */
    K next() {
        while (true) {
            // Pull a uniform random number (0 < z < 1)
            const double z = rng->nextDouble();
            const double u =
                hIntegralmaxkey + z * (hIntegralX1 - hIntegralmaxkey);
            // u is uniformly distributed in (hIntegralX1, hIntegralmaxkey]

            double x = hIntegralInverse(u);

            int k = (int)(x + 0.5);

            if (k < 1) {
                k = 1;
            } else if (k > maxkey) {
                k = maxkey;
            }

            if (k - x <= s || u >= hIntegral(k + 0.5) - h(k)) {
                if constexpr (is_sparse) {
                    return uniqueKeys[data->mapping[k] - 1];
                } else {
                    return data->mapping[k];
                }
            }
        }
    }
};

class YCSBZipfianGneratorData {
   public:
    PAD;
    int n;
    double theta;
    double alpha;
    double zeta2theta{0};
    double zetan{0};
    double eta{0};
    double ptFivePowTheta{0};
    PAD;

    YCSBZipfianGneratorData(int _n, double _theta) : theta(_theta), n(_n) {
        alpha = 1.0 / (1.0 - theta);
        zeta2theta = zeta(2, theta);
        zetan = zeta(n, theta);
        eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta2theta / zetan);
        ptFivePowTheta = pow(0.5, theta);
    }

    double zeta(int n, double theta) {
        double sum = 0;
#pragma omp parallel for schedule(static, 512) reduction(+ : sum)
        for (int i = 0; i < n; i++) { sum += pow(1.0 / (i + 1), theta); }
        return sum;
    }
};

inline uint64_t hash_64_fnv1a(const void *key, const uint64_t len) {
    const char *data = (char *)key;
    uint64_t hash = 0xcbf29ce484222325;
    uint64_t prime = 0x100000001b3;

    for (int i = 0; i < len; ++i) {
        uint8_t value = data[i];
        hash = hash ^ value;
        hash *= prime;
    }

    return hash;

}  // hash_64_fnv1a

template <typename K, bool is_sparse>
class YCSBZipfianGenerator {
    // https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java
    // This generator avoids looking up a long data array (like the other
    // zipfian generators do using std::upper_bound) which will reduce the
    // number of cache misses significantly

   private:
    PAD;
    Random64 *rng;
    YCSBZipfianGneratorData *data;
    K *uniqueKeys;
    PAD;
    K *sortedUniqueKeys;  // only used for range queries in sparse mode
    PAD;

   public:
    YCSBZipfianGenerator(Random64 *_rng, int _maxKey, double _zipfParam,
                         void *_uniqueKeys, void *_sortedUniqueKeys, void *_data)
        : rng(_rng),
          data((YCSBZipfianGneratorData *)_data),
          uniqueKeys((K *)_uniqueKeys),
          sortedUniqueKeys((K *)_sortedUniqueKeys) {
    }

    K next() {
        double u = rng->nextDouble();
        double uz = u * data->zetan;

        K ret;
        if (uz < 1.0) {
            ret = 0;
        } else if (uz < 1.0 + data->ptFivePowTheta) {
            ret = 1;
        } else {
            ret =
                (K)(data->n * pow(data->eta * u - data->eta + 1, data->alpha));
        }

        if constexpr (is_sparse) {
            return uniqueKeys[ret];
        } else {
            return 1 + (hash_64_fnv1a((void *)&ret, sizeof(K)) %
                        data->n);  // scramble the keys
            // return 1+ret;
        }
    }

    std::pair<K, K> nextRange(K rangeSize, bool respectSkew = false) {

        // Currently ignoring respectSkew because it makes it hard to pick a starting point in a way that we're sure to have enough keys in the universe WRT the range size 

        if constexpr (is_sparse) {
            int startIndex = rng->next(data->n - rangeSize + 1);
            return std::make_pair(sortedUniqueKeys[startIndex],
                                  sortedUniqueKeys[startIndex + rangeSize - 1]);
        } else {
            int start = rng->next(data->n - rangeSize + 1);
            return std::make_pair(1 + start, 1 + start + rangeSize - 1);
        }

        // if constexpr (is_sparse) {
        //     int startIndex;
        //     if (respectSkew) {
        //         K startKey = next();
        //         auto it = std::lower_bound(sortedUniqueKeys,
        //                                    sortedUniqueKeys + data->n, startKey);
        //         startIndex = it - sortedUniqueKeys;
        //         if (startIndex + rangeSize > data->n) {
        //             startIndex = data->n - rangeSize;
        //         }
        //     } else {
        //         startIndex = rng->next(data->n - rangeSize + 1);
        //     }
        //     return std::make_pair(uniqueKeys[startIndex],
        //                           uniqueKeys[startIndex + rangeSize - 1]);
        // } else {
        //     int start;
        //     if (respectSkew) {
        //         K startKey = next();
        //         start = (startKey - 1);  // convert to 0-based
        //         if (start + rangeSize > data->n) {
        //             start = data->n - rangeSize;
        //         }
        //     } else {
        //         start = rng->next(data->n - rangeSize + 1);
        //     }
        //     return std::make_pair(
        //         1 + (hash_64_fnv1a((void *)&start, sizeof(K)) % data->n),
        //         1 + (hash_64_fnv1a((void *)&(start + rangeSize - 1),
        //                             sizeof(K)) %
        //              data->n));
        //     // return std::make_pair(1+start, 1+start+rangeSize-1);
        // }
    }
};

#define PREGEN_SAMPLE_SIZE 65536

template <typename K, typename ChildKeyGenT>
class PregeneratedKeyGenerator {
   private:
    PAD;
    Random64 *rng;
    ChildKeyGenT *keygen;
    K *keys;
    PAD;
    int i;
    int stride;
    int sampleSize;
    PAD;

   public:
    PregeneratedKeyGenerator(Random64 *_rng, int _maxKey, double _zipfParam,
                             void *_uniqueKeys, void *_sortedUniqueKeys, void *_data, int _threadCount)
        : rng(_rng),
          keygen(
              new ChildKeyGenT(_rng, _maxKey, _zipfParam, _uniqueKeys, _sortedUniqueKeys, _data)),
          i(0),
          stride(1) {
        sampleSize = 5 * (_maxKey / _threadCount) + 1000;
        keys = new K[sampleSize];
        for (int64_t i = 0; i < sampleSize; i++) {
            keys[i] = keygen->next();  // the keygen handles
                                       // sparsity/zipfianness/etc internally
        }
    }

#if defined(VARIABLE_STRIDE)
#ifndef MAX_STRIDE
#define MAX_STRIDE 16
#endif
    void setRandomStride() { stride = 1 + rng->next() % MAX_STRIDE; }
#endif

#if defined(RANDOM_STARTING_POINT)
    void setRandomStartingPoint() { i = rng->next() % sampleSize; }
#endif

    K next() {
        K ret = keys[i];
        i = (i + stride) % sampleSize;
        // #if defined(VARIABLE_STRIDE) || defined(RANDOM_STARTING_POINT)
        //             if (i + stride >= sampleSize) {
        // #if defined(VARIABLE_STRIDE)
        //                 setRandomStride();
        // #endif

        // #if defined(RANDOM_STARTING_POINT)
        //                 setRandomStartingPoint();
        // #endif
        //             }
        // #endif
        return ret;
    }

    std::pair<K, K> nextRange(K rangeSize, bool respectSkew = false) {
       // empty for now
       return std::make_pair(0, 0); 
    }
};

#endif /* KEYGEN_H */

// class KeyGeneratorZipfData {
// public:
//     PAD;
//     int maxKey;
//     double c = 0; // Normalization constant
//     double* sum_probs; // Pre-calculated sum of probabilities
//     PAD;
//     KeyGeneratorZipfData(const int _maxKey, const double _alpha) {
//         COUTATOMIC("Constructing KeyGeneratorZipfData object with maxKey
//         " << _maxKey << " and alpha " << _alpha << "\n"); maxKey =
//         _maxKey;
//         // Compute normalization constant c for implied key range: [1,
//         maxKey] for (int i = 1; i <= _maxKey; i++) {
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
//     PAD;
//     KeyGeneratorZipfData * data;
//     Random64 * rng;
//     PAD;
// public:
//     KeyGeneratorZipf(KeyGeneratorZipfData * _data, Random64 * _rng)
//           : data(_data), rng(_rng) {

//           }
//     K next() {
//         double z; // Uniform random number (0 < z < 1)
//         int zipf_value = 0; // Computed exponential value to be returned
//         // Pull a uniform random number (0 < z < 1)
//         do {
//             z = (rng->next() / (double)
//             std::numeric_limits<uint64_t>::max());
//         } while ((z == 0) || (z == 1));
//         zipf_value = std::upper_bound(data->sum_probs + 1,
//         data->sum_probs + data->maxKey + 1, z) - data->sum_probs;
//         // Assert that zipf_value is between 1 and N
//         assert((zipf_value >= 1) && (zipf_value <= data->maxKey));
//         // GSTATS_ADD_IX(tid, key_gen_histogram, 1, zipf_value);
//         return (zipf_value);
//     }
// };