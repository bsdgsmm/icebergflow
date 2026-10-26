# Performance benchmarks

Two benchmarks live in [`benchmarks/run_benchmarks.py`](../benchmarks/run_benchmarks.py). Both
run in a fresh temporary warehouse, so they never touch the project's `warehouse/` directory. Each
figure is the median of several timed runs after one untimed warm-up run. The raw results,
together with the execution environment, are committed in
[`benchmarks/results/latest.json`](../benchmarks/results/latest.json). The tables below are
rendered from that file with [`benchmarks/report.py`](../benchmarks/report.py).

```bash
python benchmarks/run_benchmarks.py            # ~5 minutes on the machine below
python benchmarks/report.py --chart docs/screenshots/benchmark_layout.png
```

## Methodology

### 1. Table layout and data skipping

The benchmark generates 3,000,000 synthetic order lines with Spark (`spark.range` plus hash
functions, so the data is identical on every run) spread over 182 days. It writes them three
ways:

| Layout | Partition spec | Write order |
|---|---|---|
| `unpartitioned_shuffled` | none | random, so every file covers the whole date range |
| `unpartitioned_sorted` | none | sorted by `order_ts`, so each file covers a narrow time slice |
| `partitioned_by_day` | `days(order_ts)` | sorted by `order_ts` |

Three queries run against each layout: revenue for one day, revenue by category for one week,
and revenue by category over the full table. Besides wall time, the benchmark records Iceberg's
own scan metrics (`resultDataFiles`, `skippedDataFiles`, `totalDataFileSize`) from the executed
Spark plan. These show how much data each query actually planned to read, rather than inferring
pruning from timings.

### 2. Incremental versus full-refresh Silver processing

The benchmark generates 30 daily batches, loads the first 29 into Bronze and processes them into
Silver, then loads the 30th batch. It records the snapshot ids of every Silver table and of
`ops.watermarks`. Before each trial, those tables are rolled back to the recorded snapshots
with Iceberg's `rollback_to_snapshot` procedure, so both modes start from exactly the same
state:

* **incremental**: the default run, which reads only Bronze rows appended since the watermark.
* **full_refresh**: re-reads and re-validates all Bronze history and merges it again.

## Results

Measured on a laptop with an Intel Core i5-9300H (4 cores / 8 threads, 2.4 GHz base) and
7.9 GB RAM, running Spark in local mode.

Measured 2026-10-26T12:29:08+00:00 on Windows-10-10.0.19045-SP0, 8 logical CPUs, Python 3.12.10,
PySpark 4.0.4, Iceberg 1.10.1, Java 17.0.20.1, `local[*]`, driver memory 2g, 16 shuffle
partitions. Median of 5 runs after one warm-up.

### Table layout (3,000,000 rows over 182 days)

| Layout | Query | Median (s) | Files scanned / total | MiB scanned |
|---|---|---:|---:|---:|
| unpartitioned_shuffled | one_day_revenue | 0.280 | 13 / 13 | 34.5 |
| unpartitioned_shuffled | one_week_by_category | 0.332 | 13 / 13 | 34.5 |
| unpartitioned_shuffled | full_scan_by_category | 0.445 | 13 / 13 | 34.5 |
| unpartitioned_sorted | one_day_revenue | 0.121 | 1 / 10 | 3.1 |
| unpartitioned_sorted | one_week_by_category | 0.163 | 1 / 10 | 3.2 |
| unpartitioned_sorted | full_scan_by_category | 0.490 | 10 / 10 | 25.1 |
| partitioned_by_day | one_day_revenue | 0.118 | 1 / 182 | 0.2 |
| partitioned_by_day | one_week_by_category | 0.191 | 7 / 182 | 1.1 |
| partitioned_by_day | full_scan_by_category | 0.967 | 182 / 182 | 28.3 |

![Layout benchmark chart](screenshots/benchmark_layout.png)

### Silver processing after one new batch (30 batches in Bronze)

| Mode | Median (s) | Bronze rows read | Bronze rows total |
|---|---:|---:|---:|
| incremental | 7.885 | 2,263 | 62,252 |
| full_refresh | 13.337 | 62,252 | 62,252 |

## Interpretation

* **Pruning depends on clustering, not only on partitioning.** In the shuffled table every file
  spans the whole date range, so Iceberg's min/max column statistics cannot exclude any file and
  even a one-day query reads all 34.5 MiB. Sorting the same unpartitioned data by `order_ts` lets
  the statistics skip 9 of 10 files. Partitioning by day narrows the one-day query further to a
  single 0.2 MiB file.
* **Partitioning has a cost for scans.** The day-partitioned table holds 182 small files. A full
  scan over it took about twice as long as over the 10 to 13 larger files of the unpartitioned
  layouts, because per-file planning and open overhead dominates at this size. At 3 million rows,
  daily partitions are finer than the data justifies. Monthly partitions, or periodic compaction
  with Iceberg's `rewrite_data_files`, would keep the pruning benefit with fewer files.
* **At this scale, wall time understates the I/O difference.** For selective queries, a
  roughly 170x reduction in bytes read (34.5 MiB to 0.2 MiB) turned into a roughly 2.4x
  reduction in latency (0.280 s to 0.118 s). Most of the remaining time is Spark job scheduling
  and Iceberg planning, which do not shrink with the data. The gap in bytes read would grow
  linearly with table size; the fixed overhead would not.
* **Incremental processing reads about 3.6 % of Bronze and is about 1.7x faster.** It is not 27x
  faster, because both modes pay the same fixed cost per entity: Spark job start-up, reference
  lookups, the `MERGE INTO` commit and the watermark update. With only about 62 thousand Bronze
  rows, that fixed cost is a large part of the total. Full refresh re-reads all history, so its
  cost grows with every new batch, while the incremental cost stays roughly constant per batch.

These are single-machine, local-mode measurements on a deliberately small dataset. Use them to
compare the strategies against each other, not as absolute throughput figures.
