

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


def main(description="ArtifactInsertTimeBreakdown", skip_compilation=False):
    this_script = pathlib.Path(__file__).parent.resolve() / __file__
    
    retries = 1
    millis = 10000
    universe_size = 100_000_000
    universe_millions = universe_size // 1_000_000
    # fill_factor = 0.3
    # prefill_count = int(universe_size * fill_factor)
    prefill_count = 30_000_000
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


    base_variations = [
        art_v_nolog.with_additional_compile_def("MEASURE_COW_TIME"),
        art_v_2log.with_additional_compile_def("MEASURE_COW_TIME"),
    ]

    workloads = [
        ("WriteMostly", Workload(i=45, d=45)),
    ]

    distributions = [
        # ("Uniform", "uniform", 0.85),
        ("Zipf-99", "zipf-ycsb", 0.99),
    ]

    sparsities = [
        ("Sparse", True), 
    ][::-1] # Run sparse only because 2log doesn't do anything for dense

    thread_counts = [
        ThreadCount(nprefill=n, nwork=n, nrq=0, rqsize=0) for n in [48]
    ]


    for sparsity_str, is_sparse in sparsities: 
        for workload_str, workload in workloads: 
            for distribution_str, distribution, alpha in distributions: 
                if already_done(sparsity_str, distribution_str, workload_str):
                    print(f"Skipping {sparsity_str}-{distribution_str}-{workload_str} because already done")
                    continue

                experiment_description = f"ArtifactInsertTimeBreakdown-{distribution_str}-{workload_str}-{sparsity_str}-{universe_millions}Muniverse"

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
                        metrics.insert_time_breakdown_absolute, 
                        metrics.insert_time_breakdown_normalized_by_insert_count
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
