#include <iostream>

#include "lbtree.h"
#include "mempool.h"
#include "nvm-common.h"
#include "tree.h"

#ifdef USE_RALLOC
#include "ralloc_n1dl.hpp"
#include "ralloc_n0dl.hpp"
#endif

constexpr const auto MEMPOOL_ALIGNMENT = 4096LL;

int main() {
    initUseful();
    worker_thread_num = 1;
    worker_id = 0;

    the_thread_mempools.init(1, 4096, MEMPOOL_ALIGNMENT);
#ifdef USE_RALLOC
    RP_N0DL_init("ralloc0", 256ull * 1024 * 1024 * 1024);
    RP_N1DL_init("ralloc1", 256ull * 1024 * 1024 * 1024);
#else
    auto path_ptr =
        new std::string("/mnt/pmem1_mount/mkhalaji/lbtree_playground/file.dat");
    const char *pool_path_ = (*path_ptr).c_str();
    size_t pool_size_ = (32ULL * 1024 * 1024 * 1024);  // 32 GB
#ifdef PMEM
    std::cout << "PMEM Pool Path: " << pool_path_ << std::endl;
    std::cout << "PMEM Pool size: " << pool_size_ << std::endl;
#endif
    auto start = std::chrono::steady_clock::now();
    the_thread_nvmpools.init(1, pool_path_, pool_size_);
    auto end = std::chrono::steady_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "nvmpools init time: " << ms.count() << " ms" << std::endl;
#endif

    char *nvm_addr = (char *)nvmpool_alloc(256);

    // print the addr in a hex format
    std::cout << "nvm_addr: " << std::hex << (uintptr_t)nvm_addr << std::dec
              << std::endl;



    nvmLogInit(1);

    auto lbt = initTree(nvm_addr, false);

    simpleKeyInput input(2, 42069, 1);
    auto root = lbt->bulkload(1, &input, 1.0);  // keynum has to be at least 1
    // now delete the meme key:
    lbt->del(42069);

    std::cout << "Root: " << root << std::endl;

    std::cout << "Hello, World!" << std::endl;

    

    for (unsigned long long i = 1; i < 1000; i++) {
        lbt->insert(i, (void*)i);
    }

    return 0;
}