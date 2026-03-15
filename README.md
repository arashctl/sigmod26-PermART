# ART That Lasts (Artifacts)

For data structures with which we run into issues, there's a `STATE.md` file in their corresponding subdirectory in `ds/`. 

Scripts here to reproduce results: 

1. `artifact-01-point-operations.py`: Can be configured to reproduce figures 3, 4, 5, 6.
2. `artifact-02-range-queries.py`: Can be configured to reproduce figures 7, 8, 10. 
3. `artifact-03-insert-time-breakdown.py`: Can be configured to reproduce figure 9.

You can measure latency of operations to reproduce figures 11 and 13 by adding compile flags `MEASURE_UPDATE_LATENCY` (a misnomer, it measures latency for reads too), and `READ_UNPERSISTENT_DATA` (specific for figure 13). Keep in mind that these flags will dump a huge amount of data to the txt files, which contains the latencies of operations measured. So the throughput and other metrics aren't very reliable, but I tried my best to keep the sampling at a rate that measuring latency doesn't change the overall trends. 

You can measure the chain length by adding compile flag `TrackPtrChases` to reproduce figure 12. 