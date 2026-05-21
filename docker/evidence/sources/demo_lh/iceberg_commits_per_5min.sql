-- Commit frequency bucketed into 5-minute windows, for both Iceberg tables.
-- Uses floor(unix_seconds / 300) * 300 to align to 5-minute boundaries.
WITH snapshots AS (
    SELECT
        'transactions'                                                              AS table_name,
        committed_at,
        CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records,
        CAST(element_at(summary, 'added-data-files') AS BIGINT)                    AS added_files
    FROM iceberg.demo."transactions$snapshots"

    UNION ALL

    SELECT
        'users'                                                                     AS table_name,
        committed_at,
        CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records,
        CAST(element_at(summary, 'added-data-files') AS BIGINT)                    AS added_files
    FROM iceberg.demo."users$snapshots"
)

SELECT
    table_name,
    from_unixtime(floor(to_unixtime(committed_at) / 300.0) * 300)                 AS window_start,
    count(*)                                                                        AS commit_count,
    coalesce(sum(added_records), 0)                                                AS records_added,
    coalesce(sum(added_files), 0)                                                  AS files_added
FROM snapshots
GROUP BY 1, 2
ORDER BY 1, 2