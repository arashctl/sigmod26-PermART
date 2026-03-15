import numpy as np
from .types import RunProperties, Metric


def simple_numeric_metric(
    metrics_dict: dict, rp: RunProperties, key: str, **kwargs
) -> float:
    try:
        return float(metrics_dict[key])
    except:
        return 0


def simple_normalized_metric(
    metrics_dict: dict, rp: RunProperties, key: str, normalization_key: str, **kwargs
) -> float:
    try:
        return simple_numeric_metric(metrics_dict, rp, key) / simple_numeric_metric(
            metrics_dict, rp, normalization_key
        )
    except:
        return 0


def simple_array_metric(
    metrics_dict: dict, rp: RunProperties, key: str, **kwargs
) -> list:
    try:
        arr = metrics_dict[key].split(" ")
        return [float(x) for x in arr]
    except:
        return [0] * 100


def stacked_breakdown_metric(
    metrics_dict: dict, rp: RunProperties, key: str, **kwargs
) -> dict:
    breakdown = {}
    try:
        counted_sum = 0
        root_sum = 0
        for k, v in metrics_dict.items():
            if k.startswith(key):
                newkey = k.replace(key, "").replace("_total", "")

                if newkey != "root":
                    counted_sum += float(v)
                    breakdown[newkey] = float(v)
                else:
                    root_sum = float(v)

        breakdown["unaccounted"] = root_sum - counted_sum

        if kwargs.get("normalize_by_total", False):
            total = root_sum
            for k in breakdown:
                breakdown[k] = breakdown[k] / total if total > 0 else 0
                
        elif kwargs.get("normalize_by", None) is not None: 
            metric_to_normalize_by = kwargs.get("normalize_by")
            total = simple_numeric_metric(metrics_dict, rp, metric_to_normalize_by)
            for k in breakdown:
                breakdown[k] = breakdown[k] / total if total > 0 else 0

    except:
        pass
    return breakdown


throughput = Metric(
    label="Throughput",
    key="total_throughput",
    resolver=simple_numeric_metric,
)
update_throughput = Metric(
    label="UpdateThroughput",
    key="update_throughput",
    resolver=simple_numeric_metric,
)
find_throughput = Metric(
    label="FindThroughput",
    key="find_throughput",
    resolver=simple_numeric_metric,
)
rq_throughput = Metric(
    label="RQThroughput",
    key="rq_throughput_float",
    resolver=simple_numeric_metric,
)
total_rq = Metric(
    label="TotalRQ",
    key="total_rq",
    resolver=simple_numeric_metric,
)
iterator_throughput = Metric(
    label="IteratorThroughput",
    key="iterators_throughput_float",
    resolver=simple_numeric_metric,
)
total_iterations = Metric(
    label="TotalIterations",
    key="total_iterators",
    resolver=simple_numeric_metric,
)
query_throughput = Metric(
    label="QueryThroughput",
    key="query_throughput",
    resolver=simple_numeric_metric,
)
cache_line_flushes = Metric(
    label="CacheLineFlushes",
    key="sum_cacheline_flushes_total",
    resolver=simple_normalized_metric,
    normalization_key="total_updates",
)
total_cache_line_flushes = Metric(
    label="TotalCacheLineFlushes",
    key="sum_cacheline_flushes_total",
    resolver=simple_numeric_metric,
)
l3_misses = Metric(label="L3Misses", key="PAPI_L3_TCM", resolver=simple_numeric_metric)
ptr_chases = Metric(
    label="PtrChases",
    key="sum_vptr_chases_total",
    resolver=simple_numeric_metric,
)
ptr_chases_per_rq = Metric(
    label="PtrChasesPerRQ",
    key="sum_vptr_chases_total",
    resolver=simple_normalized_metric,
    normalization_key="total_rq",
)
ptr_chases_per_iter = Metric(
    label="PtrChasesPerIter",
    key="sum_vptr_chases_total",
    resolver=simple_normalized_metric,
    normalization_key="total_iterators",
)
versions_traversed_per_snapshot_node_read = Metric(
    label="VersionsTraversedPerSnapshotNodeRead",
    key="sum_vptr_chases_total",
    resolver=simple_normalized_metric,
    normalization_key="sum_node_snapshot_reads_total",
)
range_leaf_traversals_per_rq = Metric(
    label="RangeLeafTraversalsPerRQ",
    key="sum_range_leaf_traversals_total",
    resolver=simple_normalized_metric,
    normalization_key="total_rq",
)
insert_codepath_breakdown = Metric(
    label="InsertCodepathBreakdown",
    plot_type="stacked-breakdown",
    key="sum_insert_codepath_",  # the resolve function knows what to do with this
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
add_child_codepath_breakdown = Metric(
    label="AddChildCodepathBreakdown",
    plot_type="stacked-breakdown",
    key="sum_add_child_codepath_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
leaf_type_prevalence_breakdown = Metric(
    label="LeafTypePrevalenceBreakdown",
    plot_type="stacked-breakdown",
    key="sum_leaf_type_prevalence_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
leaf_type_prevalence_breakdown_absolute = Metric(
    label="LeafTypePrevalenceBreakdownAbsolute",
    plot_type="stacked-breakdown",
    key="sum_leaf_type_prevalence_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=False,
)
node_type_prevalence_breakdown = Metric(
    label="NodeTypePrevalenceBreakdown",
    plot_type="stacked-breakdown",
    key="sum_node_type_prevalence_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
node_type_prevalence_breakdown_absolute = Metric(
    label="NodeTypePrevalenceBreakdownAbsolute",
    plot_type="stacked-breakdown",
    key="sum_node_type_prevalence_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=False,
)
range_internal_codepath_breakdown = Metric(
    label="RangeInternalCodepathBreakdown",
    plot_type="stacked-breakdown",
    key="sum_range_internal_codepath_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
range_internal_codepath_breakdown_absolute = Metric(
    label="RangeInternalCodepathBreakdownAbsolute",
    plot_type="stacked-breakdown",
    key="sum_range_internal_codepath_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=False,
)
insert_time_breakdown = Metric(
    label="InsertTimeBreakdown",
    plot_type="stacked-breakdown",
    key="sum_insert_time_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=True,
)
insert_time_breakdown_absolute = Metric(
    label="InsertTimeBreakdownAbsolute",
    plot_type="stacked-breakdown",
    key="sum_insert_time_",
    resolver=stacked_breakdown_metric,
    normalize_by_total=False,
)
insert_time_breakdown_normalized_by_insert_count = Metric(
    label="InsertTimeBreakdownPerInsert",
    plot_type="stacked-breakdown",
    key="sum_insert_time_",
    resolver=stacked_breakdown_metric,
    normalize_by="sum_inserts_timed_total",
)
epoch_increments = Metric(
    label="EpochIncrements",
    key="sum_epoch_increments_total",
    resolver=simple_numeric_metric,
)
ralloc_alloc_calls = Metric(
    label="RallocAllocCalls",
    key="sum_ralloc_alloc_calls_total",
    resolver=simple_numeric_metric,
)
ralloc_free_calls = Metric(
    label="RallocFreeCalls",
    key="sum_ralloc_free_calls_total",
    resolver=simple_numeric_metric,
)
clear_list_calls = Metric(
    label="ClearListCalls",
    key="sum_clear_list_calls_total",
    resolver=simple_numeric_metric,
)
average_clear_batch_size = Metric(
    label="AverageClearBatchSize",
    key="sum_ralloc_free_calls_total",
    resolver=simple_normalized_metric, 
    normalization_key="sum_clear_list_calls_total",
)
clear_list_time = Metric(
    label="ClearListTime",
    key="sum_clear_list_time_total",
    resolver=simple_numeric_metric,
)
time_to_clear_each_node = Metric(
    label="TimeToClearEachNode",
    key="sum_clear_list_time_total",
    resolver=simple_normalized_metric,
    normalization_key="sum_ralloc_free_calls_total",
)
