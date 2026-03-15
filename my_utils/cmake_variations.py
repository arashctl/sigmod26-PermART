from .types import CMakeVariation
from .libraries import *


ROART = CMakeVariation(
    label="roart",
    ds_name="10-roart",
    target="Variation1",
    cmake_args=["-Dds_name=10-roart", "-DCMAKE_BUILD_TYPE=Release", "-Duse_tree_stats=0"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
    }
)

P_MassTree = CMakeVariation(
    label="p-masstree",
    ds_name="17-p-masstree",
    target="Setbench",
    cmake_args=["-Dds_name=17-p-masstree", "-DCMAKE_BUILD_TYPE=Release"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
        "VMMALLOC_POOL_DIR": "/mnt/pmem1_mount/mkhalaji/lvmlc_playground",
        "VMMALLOC_POOL_SIZE": str(90 * 1024 * 1024 * 1024),
    },
)

P_BWTree = CMakeVariation(
    label="p-bwtree",
    ds_name="18-p-bwtree",
    target="Setbench",
    cmake_args=["-Dds_name=18-p-bwtree", "-DCMAKE_BUILD_TYPE=Release"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
        "VMMALLOC_POOL_DIR": "/mnt/pmem1_mount/mkhalaji/lvmlc_playground",
        "VMMALLOC_POOL_SIZE": str(90 * 1024 * 1024 * 1024),
    },
)


P_HOT = CMakeVariation(
    label="p-hot",
    ds_name="20-p-hot",
    target="Setbench",
    cmake_args=["-Dds_name=20-p-hot", "-DCMAKE_BUILD_TYPE=Release"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
        "VMMALLOC_POOL_DIR": "/mnt/pmem1_mount/mkhalaji/lvmlc_playground",
        "VMMALLOC_POOL_SIZE": str(90 * 1024 * 1024 * 1024),
    },
)

PACTree = CMakeVariation(
    label="pactree",
    ds_name="21-pactree-sosp",
    target="Setbench",
    cmake_args=["-Dds_name=21-pactree-sosp", "-DCMAKE_BUILD_TYPE=Release"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
        "PACTREE_SOCKET0_ROOT": "/mnt/pmem0_mount/mkhalaji/pactree_playground",
        "PACTREE_SOCKET1_ROOT": "/mnt/pmem1_mount/mkhalaji/pactree_playground",
    },
)

BzTree = CMakeVariation(
    label="bztree", 
    ds_name="22-bztree",
    target="Setbench",
    cmake_args=["-Dds_name=22-bztree", "-DCMAKE_BUILD_TYPE=Release"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
        "BZTREE_PMEM_PATH": "/mnt/pmem1_mount/mkhalaji/bztree_playground/file.dat",
        "BZTREE_PMEM_SIZE": str(90 * 1024 * 1024 * 1024),
    }
)

LBTree = CMakeVariation(
    label="lb-tree", 
    ds_name="11-lbtree", 
    target="Setbench",
    cmake_args=["-Dds_name=11-lbtree", "-DCMAKE_BUILD_TYPE=Release", "-DALLOCATOR=RALLOC"],
    has_libpapi=1,
    millis=8000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True), 
    distribution="zipf-ycsb",
    prefill_mode="-prefill-insert",
    alpha=0.99,
    perf=False,
    txn=False,
    environ={
    }
)