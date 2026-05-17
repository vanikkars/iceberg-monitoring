---
title: Iceberg Table Health
---

Monitors the health of the two Iceberg tables (`transactions`, `users`) written by the Flink pipeline.
Data is sourced from Trino's built-in Iceberg metadata tables (`$snapshots`, `$files`, `$manifests`).

---

## Current State KPIs

```sql kpi_txn
select * from demo_lh.iceberg_health_kpis where table_name = 'transactions'
```

```sql kpi_usr
select * from demo_lh.iceberg_health_kpis where table_name = 'users'
```

### transactions

<Grid cols=4>
  <BigValue data={kpi_txn} value=total_records           title="Total Records" />
  <BigValue data={kpi_txn} value=total_commits           title="Total Commits" />
  <BigValue data={kpi_txn} value=avg_file_size_kb        title="Avg File Size (KB)" />
  <BigValue data={kpi_txn} value=avg_records_per_file    title="Avg Records / File" />
  <BigValue data={kpi_txn} value=total_data_files        title="Current Data Files" />
  <BigValue data={kpi_txn} value=total_size_mb           title="Total Size (MB)" />
  <BigValue data={kpi_txn} value=avg_records_per_commit  title="Avg Records / Commit" />
  <BigValue data={kpi_txn} value=minutes_since_last_commit title="Mins Since Last Commit" />
</Grid>

### users

<Grid cols=4>
  <BigValue data={kpi_usr} value=total_records           title="Total Records" />
  <BigValue data={kpi_usr} value=total_commits           title="Total Commits" />
  <BigValue data={kpi_usr} value=avg_file_size_kb        title="Avg File Size (KB)" />
  <BigValue data={kpi_usr} value=avg_records_per_file    title="Avg Records / File" />
  <BigValue data={kpi_usr} value=total_data_files        title="Current Data Files" />
  <BigValue data={kpi_usr} value=total_size_mb           title="Total Size (MB)" />
  <BigValue data={kpi_usr} value=avg_records_per_commit  title="Avg Records / Commit" />
  <BigValue data={kpi_usr} value=minutes_since_last_commit title="Mins Since Last Commit" />
</Grid>

---

## Commit Activity

Flink checkpoints every 30 seconds, so each 5-minute window should contain ~10 commits per table.
A gap or drop in commit count indicates a stalled pipeline or checkpoint failure.

```sql commits_5min
select * from demo_lh.iceberg_commits_per_5min
```

<LineChart
  data={commits_5min}
  x=window_start
  y=commit_count
  series=table_name
  title="Commits per 5-Minute Window"
  yAxisTitle="Commits"
/>

<LineChart
  data={commits_5min}
  x=window_start
  y=records_added
  series=table_name
  title="Records Written per 5-Minute Window"
  yAxisTitle="Records Added"
/>

---

## File Size Trends

A healthy streaming table written by Flink will produce many small files (one per checkpoint).
`avg_added_file_size_kb` tracks the size of newly written files.
`eod_avg_file_size_kb` tracks the overall average across all live files at end of day.

```sql daily_stats
select * from demo_lh.iceberg_daily_stats
```

```sql monthly_stats
select
    table_name,
    date_trunc('month', day)                                     as month,
    sum(commit_count)                                            as commit_count,
    sum(total_added_records)                                     as total_added_records,
    round(avg(avg_added_file_size_kb), 2)                       as avg_added_file_size_kb,
    round(avg(eod_avg_file_size_kb), 2)                         as avg_eod_file_size_kb
from demo_lh.iceberg_daily_stats
group by table_name, month
order by table_name, month
```

### Avg File Size per Day (KB)

<LineChart
  data={daily_stats}
  x=day
  y=avg_added_file_size_kb
  series=table_name
  title="Avg Size of Newly Added Files per Day (KB)"
  yAxisTitle="KB"
/>

### Avg File Size per Month (KB)

<BarChart
  data={monthly_stats}
  x=month
  y=avg_added_file_size_kb
  series=table_name
  title="Avg Size of Newly Added Files per Month (KB)"
  yAxisTitle="KB"
/>

---

## Records Growth

```sql records_growth
select * from demo_lh.iceberg_records_growth
```

<LineChart
  data={records_growth}
  x=committed_at
  y=total_records
  series=table_name
  title="Total Records Over Time"
  yAxisTitle="Total Records"
/>

<LineChart
  data={records_growth}
  x=committed_at
  y=total_data_files
  series=table_name
  title="Total Data Files Over Time"
  yAxisTitle="Data Files"
/>

---

## Current File Distribution (from `$files`)

Compaction is needed when `avg_records_per_file` is low and `file_count` is high.
Flink with 30-second checkpoints will naturally produce many small files until compacted.

```sql file_size_current
select * from demo_lh.iceberg_file_size_current
```

<DataTable
  data={file_size_current}
  rows=10
>
  <Column id=table_name        title="Table" />
  <Column id=file_count        title="Files" />
  <Column id=total_records     title="Total Records" />
  <Column id=avg_records_per_file title="Avg Records/File" />
  <Column id=min_records_per_file title="Min Records/File" />
  <Column id=max_records_per_file title="Max Records/File" />
  <Column id=avg_file_size_kb  title="Avg Size (KB)" />
  <Column id=min_file_size_kb  title="Min Size (KB)" />
  <Column id=max_file_size_kb  title="Max Size (KB)" />
  <Column id=total_size_mb     title="Total Size (MB)" />
</DataTable>

---

## Manifest Health (from `$manifests`)

`active_manifest_count` = manifests referenced by the current snapshot.
`total_snapshot_count` = total commits ever made (each append adds one manifest).
`est_orphaned_manifests` ≈ accumulated unreferenced manifests not yet cleaned up.
`pending_deleted_files` = data files logically deleted but not physically removed — run `expireSnapshots` to reclaim storage.

```sql manifest_health
select * from demo_lh.iceberg_manifest_health
```

<DataTable data={manifest_health}>
  <Column id=table_name              title="Table" />
  <Column id=active_manifest_count   title="Active Manifests" />
  <Column id=total_snapshot_count    title="Total Snapshots" />
  <Column id=est_orphaned_manifests  title="Est. Orphaned Manifests" />
  <Column id=existing_data_files     title="Existing Data Files" />
  <Column id=pending_deleted_files   title="Pending Deleted Files" />
  <Column id=pending_deleted_rows    title="Pending Deleted Rows" />
</DataTable>

<details>
<summary>SQL source definitions</summary>

```sql iceberg_health_kpis
select * from demo_lh.iceberg_health_kpis
```

```sql iceberg_commits_per_5min
select * from demo_lh.iceberg_commits_per_5min
```

```sql iceberg_daily_stats_full
select * from demo_lh.iceberg_daily_stats
```

```sql iceberg_manifest_health_full
select * from demo_lh.iceberg_manifest_health
```

```sql iceberg_file_size_current_full
select * from demo_lh.iceberg_file_size_current
```

```sql iceberg_records_growth_full
select * from demo_lh.iceberg_records_growth
```

</details>