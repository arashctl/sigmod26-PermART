
# This script runs point operations for the paper. 
# I'm not using the pre-created cmake variations because I want this to be standalone (and reproducible in the future).

#! /usr/bin/python3

import dis
from os import environ
import pathlib, sys
from my_utils.types import (
    CMakeVariation,
    Config,
    Metric,
    ThreadCount,
    Variation,
    Workload,
    MakeVariation,
)
import my_utils.metrics as metrics
from my_utils.utils import Experiment
import copy

# For resuming 
def already_done(sparsity, distribution, workload): 
    return False



def main(description="ArtifactPointQueries", skip_compilation=False):
    this_script = pathlib.Path(__file__).parent.resolve() / __file__
    
    retries = 1
    millis = 10000
    universe_size = 100_000_000
    universe_millions = universe_size // 1_000_000
    fill_factor = 0.3
    prefill_count = int(universe_size * fill_factor)
    universe_coefficient = (universe_size) / (prefill_count)


    art_v_nolog = CMakeVariation(
        label="PermART",
        ds_name="04-artifact",
        target="Variation2",
        cmake_args=[
            "-Dds_name=04-artifact",
            "-DART_VARIATION=Variation2",
            "-DALLOCATOR=RALLOC_NUMA",
            "-DCMAKE_BUILD_TYPE=Release",
        ],
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
        },
    )

    art_v_2log = CMakeVariation(
        label="PermART-2Log",
        ds_name="04-artifact",
        target="Variation5",
        cmake_args=[
            "-Dds_name=04-artifact",
            "-DART_VARIATION=Variation5",
            "-DALLOCATOR=RALLOC_NUMA",
            "-DCMAKE_BUILD_TYPE=Release",
        ],
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
        },
    )


    ROART = CMakeVariation(
        label="ROART",
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

    roart_dcmm = ROART.replace(
        label="roart-dcmm",
    ).with_allocator("DCMM")

    roart_pmdk = ROART.replace(
        label="roart-pmdk",
    ).with_allocator("PMDK")

    roart_ralloc = ROART.replace(
        label="ROART-Ralloc",
    ).with_allocator("RALLOC_NUMA")

    pactree = CMakeVariation(
        label="PACTree",
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

    # This is the good one
    pactree_reverse_socket = pactree.with_additional_compile_def("PREFER_SOCKET0").replace(
        label="PACTree"
    )

    pactree_reverse_socket_ralloc = pactree_reverse_socket.with_allocator("RALLOC").replace(
        label="PACTree-Ralloc",
    )

    masstree = CMakeVariation(
        label="P-Masstree",
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

    masstree_ralloc = masstree.with_allocator("RALLOC").replace(
        label="P-Masstree-Ralloc",
        environ=dict()
    )

    base_variations = [
        art_v_nolog, 
        art_v_2log,
        # roart_dcmm,
        roart_ralloc, 
        # pactree_reverse_socket,
        pactree_reverse_socket_ralloc,
        masstree_ralloc
    ]

    workloads = [
        # ("ReadHeavy", Workload(i=2.5, d=2.5)),
        # ("ReadMostly", Workload(i=5, d=5)),
        ("Balanced", Workload(i=25, d=25)),
        # ("WriteMostly", Workload(i=45, d=45)),
        # ("WriteHeavy", Workload(i=47.5, d=47.5)),
    ]

    distributions = [
        # ("Uniform", "uniform", 0.85),
        ("Zipf-99", "zipf-ycsb", 0.99),
        # ("Zipf-75", "zipf-ycsb", 0.75),
    ]

    sparsities = [
        # ("Sparse", True), 
        ("Dense", False)
    ][::-1] # Run dense first as they are faster

    thread_counts = [
        ThreadCount(nprefill=n, nwork=n, nrq=0, rqsize=0) for n in [48]
    ]



    for sparsity_str, is_sparse in sparsities: 
        for workload_str, workload in workloads: 
            for distribution_str, distribution, alpha in distributions: 
                if already_done(sparsity_str, distribution_str, workload_str):
                    print(f"Skipping {sparsity_str}-{distribution_str}-{workload_str} because already done")
                    continue

                experiment_description = f"ArtifactPointQueries-{distribution_str}-{workload_str}-{sparsity_str}-{universe_millions}Muniverse"

                variations = [copy.deepcopy(v) for v in base_variations]
                actual_variations = []
                for v in variations:
                    actual_variations.append(
                        v.replace(
                            prefill_count=prefill_count,
                            universe_coefficient=universe_coefficient,
                            millis=millis,
                            distribution=distribution,
                            alpha=alpha,
                            additional_run_args="-sparse" if is_sparse else "",
                        )
                    )
                config = Config(
                    description=experiment_description,
                    workloads=(workload,), # separating experiment per workload is cleaner
                    variations=actual_variations,
                    metrics=[
                        metrics.throughput,
                        metrics.update_throughput,
                        metrics.find_throughput,
                        metrics.l3_misses
                    ],
                    thread_counts=thread_counts,
                    retries=retries,
                )

                exp = Experiment(
                    config,
                    script_path=this_script,
                )
                exp.run(skip_compilation=skip_compilation)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        skip_compilation = False
        if len(sys.argv) == 3:
            skip_compilation = sys.argv[2].lower() in ["y", "yes", "true", "1"]

        main(description=sys.argv[1], skip_compilation=skip_compilation)
    else:
        main()
