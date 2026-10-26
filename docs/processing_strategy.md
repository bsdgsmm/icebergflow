# Processing strategy

This document explains how IcebergFlow keeps every layer correct when runs are repeated,
interrupted or fed late, duplicated or differently shaped data. Each guarantee below is covered
by an integration test that runs real Spark jobs against real Iceberg tables.

## Idempotent Bronze ingestion

A Bronze load appends a batch and, in the same commit, records the batch id as an Iceberg
snapshot summary property:

```python
(
    df.writeTo("bronze.orders")
    .option("snapshot-property.icebergflow.batch-id", "2024-01-05")
    .option("snapshot-property.icebergflow.source-rows", "385")
    .append()
)
```

Before loading, the ingester reads the batch ids recorded on the snapshots in the table's
**current lineage** (`history.is_current_ancestor`). Because the marker and the data are a
single atomic commit, a batch is either completely present with its marker or completely absent.
There is no window in which data exists without the marker, which a separate audit table written
after the data would have. Consequences:

* Rerunning `icebergflow ingest` or `icebergflow run` is a no-op for loaded batches. No new
  snapshots are created (`test_rerun_is_a_no_op`).
* A crash part-way through a run can simply be retried. Already committed `(table, batch)`
  pairs are skipped and the rest are loaded (`test_resume_after_partial_failure`).
* Rolling a Bronze table back with `CALL lakehouse.system.rollback_to_snapshot(...)` also
  removes the markers of the rolled-back batches, so the next run reloads them
  (`test_rolled_back_batch_is_reloaded`).
* A batch whose row count does not match its manifest is rejected before anything is committed
  (`test_row_count_mismatch_rejects_batch_without_commit`).

Bronze is append-only by design. A corrected re-delivery of a batch should arrive under a new
batch id (for example `2024-01-05-r1`, which the batch pattern accepts). Silver then resolves the
newest version per key.

## Incremental Silver processing

Silver keeps one watermark per entity in `ops.watermarks`: the id of the last Bronze snapshot it
has fully processed. On each run:

1. Read the current Bronze snapshot `S` and the watermark `W`.
2. If `W = S`, nothing is read or written for that entity.
3. If Bronze descends from `W` through **append** snapshots only, read just the rows added in
   `(W, S]` with an Iceberg incremental append scan:

   ```python
   (
       spark.read.format("iceberg")
       .option("start-snapshot-id", W)
       .option("end-snapshot-id", S)
       .load("lakehouse.bronze.orders")
   )
   ```

4. Otherwise (no watermark, or Bronze was rolled back, overwritten or had rows deleted), read
   the full table as of `S`.
5. Validate, quarantine, deduplicate and `MERGE INTO` Silver, then set the watermark to `S`.

Both writes in step 5 are idempotent. The quarantine merge is keyed on (entity, batch,
record hash), and the Silver merge only replaces a row with a strictly newer version. If the
process dies after the merge but before the watermark update, the next run re-reads the same
range and converges to the same state (`test_crash_before_watermark_update_is_recovered`). The
tests also check that processing batches one at a time produces exactly the same Silver and Gold
content as a full refresh (`test_incremental_result_equals_full_refresh`).

`icebergflow silver --full-refresh` and `icebergflow run --full-refresh` ignore the watermarks
and re-merge all of Bronze. This is useful after changing validation rules.

## Gold refresh

Gold models are SQL `SELECT` statements in [`sql/gold`](../sql/gold). Each rebuild is written as
a single Iceberg commit, so readers never observe a half-written model, and every previous
version remains available with `VERSION AS OF`.

* `daily_sales` and `channel_daily` are partitioned by `months(order_date)`. After an
  incremental Silver run, the pipeline collects the months of every order or order line merged
  in that run (`_processed_at >= run start`). It then recomputes only those months with a
  dynamic partition overwrite (`overwritePartitions`). Data files of all other months stay
  untouched, and the test suite compares their file paths before and after.
* The remaining models depend on full history (lifetime value, ranks, cohorts) and are rebuilt
  completely. They are small aggregates, so the full rebuild is cheap.
* If a run merged nothing into Silver, Gold is a pure function of unchanged inputs and is
  skipped entirely, so reruns create no new Gold snapshots
  (`test_rerun_without_new_data_leaves_gold_untouched`).

## Quality gates

After Silver is updated, the Silver check suite runs. If any `error`-level check fails, the run
stops before Gold is touched, so the previously published Gold tables remain the latest
consistent version (`test_failed_silver_gate_leaves_gold_untouched`). After Gold is rebuilt, the
Gold suite reconciles it against Silver. All results are appended to `ops.dq_results`. See the
[README](../README.md#data-quality-and-testing) for the list of check types.

## Schema evolution

| Layer | Behaviour |
|---|---|
| Bronze | New source columns are added automatically (`ALTER TABLE ... ADD COLUMN ... AFTER`). Names must be lower snake case; anything else fails the batch with `SchemaDriftError`. Columns missing from a batch are stored as NULL. |
| Silver | The target schema is declared in code. `ensure_silver_tables` adds missing columns to existing tables in place. Optional source columns absent from older batches are read as NULL. |
| Gold | Models select explicit columns, so new Silver columns only appear in Gold when a model uses them (for example `channel_daily`). |

Iceberg column additions change metadata only: no data file is rewritten, and older files read
the new columns as NULL. Snapshots taken before a change keep their schema:

```sql
SELECT * FROM bronze.orders VERSION AS OF 8151679875795268000;  -- columns as of that snapshot
```

The tests also document a subtle difference: the DataFrame reader option
`spark.read.option("snapshot-id", ...)` selects the old snapshot's files but projects the
**current** schema, so new columns appear as NULL. SQL `VERSION AS OF` returns the snapshot's
original schema.

## Partition evolution

Partitioning can change without rewriting data:

```bash
icebergflow evolve-partition silver.order_items "bucket(8, order_id)"
```

Existing files keep their original (unpartitioned) spec, and new files are written with the new
spec. Queries read both transparently, and filters on `order_id` prune the new files
(`test_partition_evolution_keeps_old_files_and_prunes_new_ones`).

## Failure handling summary

| Failure | Effect | Recovery |
|---|---|---|
| Missing or truncated source file | Batch fails before commit | Fix the file and rerun |
| Malformed JSON line | Kept in Bronze `_corrupt_record`, quarantined in Silver | Inspect `silver.quarantine` |
| Invalid record | Quarantined with all reasons | Correct the source, deliver a new batch |
| Crash during Bronze load | Committed batches stay, the rest is missing | Rerun |
| Crash between Silver merge and watermark | Same range is re-read | Rerun |
| Silver quality gate fails | Gold not rebuilt | Fix the data or the rule, then rerun |
| Bad data committed | | `rollback_to_snapshot`, then rerun; Silver falls back to a full read |
