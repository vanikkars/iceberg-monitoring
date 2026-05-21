-- Current-state health KPIs for each Iceberg table.
-- Derived from the latest snapshot summary (standard Iceberg keys written by Flink).
WITH txn_latest AS (
    SELECT
        committed_at,
        CAST(element_at(summary, 'total-records')    AS BIGINT) AS total_records,
        CAST(element_at(summary, 'total-data-files') AS BIGINT) AS total_data_files,
        CAST(element_at(summary, 'total-files-size') AS BIGINT) AS total_files_size_bytes
    FROM iceberg.demo."transactions$snapshots"
    ORDER BY committed_at DESC
    LIMIT 1
),
txn_agg AS (
    SELECT
        count(*) AS total_commits,
        round(avg(CAST(element_at(summary, 'added-records') AS DOUBLE)), 0) AS avg_records_per_commit
    FROM iceberg.demo."transactions$snapshots"
),
usr_latest AS (
    SELECT
        committed_at,
        CAST(element_at(summary, 'total-records')    AS BIGINT) AS total_records,
        CAST(element_at(summary, 'total-data-files') AS BIGINT) AS total_data_files,
        CAST(element_at(summary, 'total-files-size') AS BIGINT) AS total_files_size_bytes
    FROM iceberg.demo."users$snapshots"
    ORDER BY committed_at DESC
    LIMIT 1
),
usr_agg AS (
    SELECT
        count(*) AS total_commits,
        round(avg(CAST(element_at(summary, 'added-records') AS DOUBLE)), 0) AS avg_records_per_commit
    FROM iceberg.demo."users$snapshots"
)

SELECT
    'transactions'                                                                  AS table_name,
    t.total_records,
    t.total_data_files,
    round(CAST(t.total_files_size_bytes AS DOUBLE) / 1024.0 / 1024.0, 2)          AS total_size_mb,
    CASE WHEN t.total_data_files > 0
         THEN round(CAST(t.total_files_size_bytes AS DOUBLE) / t.total_data_files / 1024.0, 2)
         ELSE NULL END                                                              AS avg_file_size_kb,
    CASE WHEN t.total_data_files > 0
         THEN round(CAST(t.total_records AS DOUBLE) / t.total_data_files, 0)
         ELSE NULL END                                                              AS avg_records_per_file,
    a.total_commits,
    a.avg_records_per_commit,
    t.committed_at                                                                  AS last_commit_at,
    date_diff('minute', t.committed_at, now())                                     AS minutes_since_last_commit
FROM txn_latest t, txn_agg a

UNION ALL

SELECT
    'users'                                                                         AS table_name,
    u.total_records,
    u.total_data_files,
    round(CAST(u.total_files_size_bytes AS DOUBLE) / 1024.0 / 1024.0, 2)          AS total_size_mb,
    CASE WHEN u.total_data_files > 0
         THEN round(CAST(u.total_files_size_bytes AS DOUBLE) / u.total_data_files / 1024.0, 2)
         ELSE NULL END                                                              AS avg_file_size_kb,
    CASE WHEN u.total_data_files > 0
         THEN round(CAST(u.total_records AS DOUBLE) / u.total_data_files, 0)
         ELSE NULL END                                                              AS avg_records_per_file,
    a.total_commits,
    a.avg_records_per_commit,
    u.committed_at                                                                  AS last_commit_at,
    date_diff('minute', u.committed_at, now())                                     AS minutes_since_last_commit
FROM usr_latest u, usr_agg a