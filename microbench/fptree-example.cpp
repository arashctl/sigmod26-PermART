#include <iostream>
#include "fptree.h"

int main() {
    std::cout << "{Hello, World!}" << std::endl;
    // FPTree::pmemInit("test_pool", 1024*1024*1024);
    FPtree tree; 
    tree.pmemInit("/mnt/pmem1_mount/mkhalaji/pmemobj_playground/file.dat", 24*1024*1024*1024ULL);

    std::cout << tree.find(124) << std::endl;

    struct KV kv = {124, 125};
    tree.insert(kv); 

    std::cout << tree.find(124) << std::endl;

    return 0;
}