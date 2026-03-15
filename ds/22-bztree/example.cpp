#include <bits/stdint-uintn.h>
#include <string.h>

#include <iostream>
#include <string>

#include "bztree.h"

std::string get_pmem_path() {
    const char *pmem_path = std::getenv("BZTREE_PMEM_PATH");
    if (pmem_path == nullptr) {
        return "/mnt/pmem1_mount/mkhalaji/bztree_playground/file.dat";
    }
    return std::string(pmem_path);
}

auto get_pmem_size() {
    const char *pmem_size_str = std::getenv("BZTREE_PMEM_SIZE");
    if (pmem_size_str == nullptr) {
        return 10ull * 1024 * 1024 * 1024;  // 45 GB
    }
    return std::stoull(pmem_size_str);
}


static constexpr int kDescriptorsPerThread = 1024; 

int main() {
    std::cout << "BzTree Example" << std::endl;
    bztree::BzTree *bztree;
    bztree::BzTree::ParameterSet param(1024, 512, 1024);
    uint32_t num_threads = 2; 
    uint32_t desc_pool_size = kDescriptorsPerThread * num_threads;

#ifdef PMDK
    pmwcas::InitLibrary(
        pmwcas::PMDKAllocator::Create(get_pmem_path().c_str(), "bztree_layout",
                                      get_pmem_size()),
        pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
        pmwcas::LinuxEnvironment::Destroy);

    auto pmdk_allocator =
        reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
    bztree::Allocator::Init(pmdk_allocator);

    bztree = reinterpret_cast<bztree::BzTree *>(
        pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
    pmdk_allocator->Allocate((void **)&bztree->pmwcas_pool,
                             sizeof(pmwcas::DescriptorPool));
    new (bztree->pmwcas_pool)
        pmwcas::DescriptorPool(desc_pool_size, num_threads, false);

    new (bztree)
        bztree::BzTree(param, bztree->pmwcas_pool,
                       reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));

#else
    pmwcas::InitLibrary(
        pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
        auto pool = new pmwcas::DescriptorPool(2000, num_threads, false);
        bztree = bztree::BzTree::New(param, pool);

#endif

    
    size_t op_checksum = 0;

    for (int i = 1; i < 2000; i++) {
        std::string key = std::to_string(i);
        auto res = bztree->Insert(key.c_str(), key.size(), i * 10);
        // std::cout << "Insert " << i << " : " << res.IsOk() << std::endl;
        if (res.IsOk()) {
            op_checksum += i;
        }
    }

    // Let's reinsert them (with different values) to see if duplicate keys are
    // handled correctly
    for (int i = 1; i < 2000; i += 3) {
        std::string key = std::to_string(i);
        auto res = bztree->Insert(key.c_str(), key.size(), i * 90);
        // std::cout << "Reinsert " << i << " : " << res.IsOk() << std::endl;
        if (res.IsOk()) {
            op_checksum += i;
        }
    }

    // Let's delete some keys
    for (int i = 1; i < 500; i += 3) {
        std::string key = std::to_string(i);
        auto res = bztree->Delete(key.c_str(), key.size());
        // std::cout << "Delete " << i << " : " << res.IsOk() << std::endl;
        if (res.IsOk()) {
            op_checksum -= i;
        }
    }
    // Let's do a few lookups:
    for (int i = 0; i < 2000; i++) {
        std::string key = std::to_string(i);
        uint64_t payload;
        auto res = bztree->Read(key.c_str(), key.size(), &payload);

        // if (res.IsOk()) {
        //     std::cout << "Key " << i << " was found" << std::endl;
        // } else {
        //     std::cout << "Key " << i << " was not found" << std::endl;
        // }
    }


    // Let's insert a few of those deleted keys again:
    for (int i = 1; i < 500; i++) {
        std::string key = std::to_string(i);
        auto res = bztree->Insert(key.c_str(), key.size(), i * 100);
        // std::cout << "Reinsert deleted " << i << " : " << res.IsOk()
                //   << std::endl;
        if (res.IsOk()) {
            op_checksum += i;
        }
    }

    size_t read_checksum = 0;

    // Let's iterate over all keys: 
    auto iter = bztree->RangeScanBySize("1", 1, 3000);
    while (true) {
        auto record = iter->GetNext();
        if (record == nullptr) break;

        read_checksum += std::stoull(std::string(record->GetKey(), record->GetKey() + record->meta.GetKeyLength()));

        std::string key_str(record->GetKey(), record->GetKey() + record->meta.GetKeyLength());
        // std::cout << "Iterate key: " << key_str << " with payload: " << record->GetPayload() << std::endl;
    }   

    std::cout << "Op checksum: " << op_checksum << ", Read checksum: " << read_checksum << std::endl;
    if (op_checksum == read_checksum) {
        std::cout << "Checksums match!" << std::endl;
    } else {
        std::cout << "Checksums do not match!" << std::endl;
    }

    return 0;
}