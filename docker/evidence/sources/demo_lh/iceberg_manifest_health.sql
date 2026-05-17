-- Manifest health for each Iceberg table.
-- active_manifest_count : manifests referenced by the current snapshot
-- total_snapshot_count  : total commits ever made (each adds ~1 manifest to history)
-- est_orphaned_manifests: snapshots that have accumulated without manifest compaction
--                         (total_snapshot_count - active_manifest_count)
-- pending_deleted_files : data files logically deleted but not yet physically removed
--                         → run snapshot expiry + orphan file cleanup to reclaim space
WITH txn_manifests AS (
    SELECT
        'transactions'                              AS table_name,
        count(*)                                    AS active_manifest_count,
        sum(added_data_files_count)                 AS added_data_files,
        sum(existing_data_files_count)              AS existing_data_files,
        sum(deleted_data_files_count)               AS pending_deleted_files,
        sum(added_rows_count)                       AS added_rows,
        sum(existing_rows_count)                    AS existing_rows,
        sum(deleted_rows_count)                     AS pending_deleted_rows
    FROM iceberg.demo."transactions$manifests"
),
txn_snap_count AS (
    SELECT count(*) AS total_snapshot_count
    FROM iceberg.demo."transactions$snapshots"
),
usr_manifests AS (
    SELECT
        'users'                                     AS table_name,
        count(*)                                    AS active_manifest_count,
        sum(added_data_files_count)                 AS added_data_files,
        sum(existing_data_files_count)              AS existing_data_files,
        sum(deleted_data_files_count)               AS pending_deleted_files,
        sum(added_rows_count)                       AS added_rows,
        sum(existing_rows_count)                    AS existing_rows,
        sum(deleted_rows_count)                     AS pending_deleted_rows
    FROM iceberg.demo."users$manifests"
),
usr_snap_count AS (
    SELECT count(*) AS total_snapshot_count
    FROM iceberg.demo."users$snapshots"
)

SELECT
    m.table_name,
    m.active_manifest_count,
    s.total_snapshot_count,
    greatest(0, s.total_snapshot_count - m.active_manifest_count)   AS est_orphaned_manifests,
    m.added_data_files,
    m.existing_data_files,
    m.pending_deleted_files,
    m.added_rows,
    m.existing_rows,
    m.pending_deleted_rows
FROM txn_manifests m, txn_snap_count s

UNION ALL

SELECT
    m.table_name,
    m.active_manifest_count,
    s.total_snapshot_count,
    greatest(0, s.total_snapshot_count - m.active_manifest_count)   AS est_orphaned_manifests,
    m.added_data_files,
    m.existing_data_files,
    m.pending_deleted_files,
    m.added_rows,
    m.existing_rows,
    m.pending_deleted_rows
FROM usr_manifests m, usr_snap_count s