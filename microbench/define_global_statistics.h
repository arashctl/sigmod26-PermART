/*
 * File:   define_global_statistics.h
 * Author: t35brown
 *
 * Created on August 5, 2019, 5:25 PM
 */

#ifndef GSTATS_OUTPUT_DEFS_H
#define GSTATS_OUTPUT_DEFS_H

// gstats_handle_stat(LONG_LONG, num_recmgr_startOp_calls, 1, { \
//             gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
//       __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
//     }) \
//     gstats_handle_stat(LONG_LONG, num_rmset_startOp_calls, 1, { \
//             gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
//       __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
//     }) \
//     gstats_handle_stat(LONG_LONG, num_debra_startOp_calls, 1, { \
//             gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
//       __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
//     }) \
//     gstats_handle_stat(LONG_LONG, num_rmset_getReclaimers_calls, 1, { \
//             gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
//       __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
//     }) \

#define USE_GSTATS
#ifndef __AND
#   define __AND ,
#endif
#define GSTATS_HANDLE_STATS(gstats_handle_stat) \
    gstats_handle_stat(LONG_LONG, hand_over_hand_range_histogram, 50, { \
            gstats_output_item(PRINT_RAW, SUM, BY_INDEX) \
            __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, version_chain_traversal_histogram, 10000, { \
            gstats_output_item(PRINT_RAW, SUM, BY_INDEX) \
            __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, range_return_size, 1024, { \
            gstats_output_item(PRINT_RAW, SUM, BY_INDEX) \
            __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
gstats_handle_stat(LONG_LONG, epoch_increments, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
     gstats_handle_stat(LONG_LONG, inserts_timed, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, clear_list_calls, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, ralloc_alloc_calls, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, ralloc_free_calls, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_time_root, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, clear_list_time, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_leaf_cow, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_internal_cow, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_split_cow, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_new_node, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_log_append, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_log_timestamp, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_node_timestamp, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, insert_time_lock_wait, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
    gstats_handle_stat(LONG_LONG, vptr_chases, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, range_internal_codepath_root, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, range_internal_codepath_isnull, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_full, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_indirect, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_sparse, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_sleaf, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_bleaf, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_log_nologs, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_log_first_log_only, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_log_both_logs, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, range_internal_codepath_no_overlap, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_root, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_key_exists, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_retry, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, insert_codepath_retry_after_lock, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_add_to_empty, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_append_log, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_cow_s2s, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_cow_s2b, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_cow_b2b, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_cow_b2sparse, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_add_to_nonleaf, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, insert_codepath_add_child, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
 gstats_handle_stat(LONG_LONG, insert_codepath_rare_case_key_found, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_root, 1, { \
        gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, remove_codepath_no_child, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_key_not_found, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_retry, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_set_null, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, remove_codepath_append_log, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, remove_codepath_log2null, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_cow_b2b, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_cow_b2s, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_cow_log2sparse, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_cow_s2s, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, remove_codepath_rare_case_key_not_found, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, append_codepath_root, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, append_codepath_first_log, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, append_codepath_second_log, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, append_codepath_persist_root, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, append_codepath_flush_log_entry, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, append_codepath_clear_unpersisted_bit, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, add_child_codepath_root, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, add_child_codepath_cow_i2f, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, add_child_codepath_cow_i2i, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, add_child_codepath_cow_s2i, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, add_child_codepath_cow_s2s, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_small, 1, { \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_big, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_log, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_root, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_full, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_indirect, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, node_type_prevalence_sparse, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
    gstats_handle_stat(LONG_LONG, node_snapshot_reads, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, range_leaf_traversals, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, all_leaf_finds, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, ruled_out_leaf_finds, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, all_node_allocations, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, pmem_node_allocations, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, lock_contention, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, spin_reloads, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, persistent_helps, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, add_child_calls, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, cacheline_flushes, 1, { \
        gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, key_gen_histogram, 200, { \
            gstats_output_item(PRINT_RAW, SUM, BY_INDEX) \
    }) \
    gstats_handle_stat(LONG_LONG, num_allocations, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_inserts, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_deletes, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_successful_inserts, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_successful_deletes, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_searches, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
        gstats_handle_stat(LONG_LONG, num_iterators, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_rq, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_operations, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, AVERAGE, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, STDEV, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, size_checksum, 1, {}) \
    gstats_handle_stat(LONG_LONG, key_checksum, 1, {}) \
    gstats_handle_stat(LONG_LONG, prefill_size, 1, {}) \
    gstats_handle_stat(LONG_LONG, time_thread_terminate, 1, { \
            gstats_output_item(PRINT_RAW, FIRST, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, time_thread_start, 1, { \
            gstats_output_item(PRINT_RAW, FIRST, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, timer_duration, 1, {}) \
    gstats_handle_stat(LONG_LONG, duration_all_ops, 1, { /* note: used by brown_ext_ist_lf */ \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    






#endif /* GSTATS_OUTPUT_DEFS_H */
