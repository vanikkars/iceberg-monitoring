-- Per-day rollup of Iceberg commit activity and file size metrics.
-- avg_file_size_kb = avg size of files added on that day (added-files-size / added-data-files).
-- end_of_day_avg_file_size_kb = size of all files at the last snapshot of the day
--   (total-files-size / total-data-files), showing the running file size trend.
WITH snapshots AS (
    SELECT
        'transactions'                                                              AS table_name,
        committed_at,
        CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records,
        CAST(element_at(summary, 'added-data-files') AS BIGINT)                    AS added_data_files,
        CAST(element_at(summary, 'added-files-size') AS BIGINT)                    AS added_files_size_bytes,
        CAST(element_at(summary, 'total-records')    AS BIGINT)                    AS total_records,
        CAST(element_at(summary, 'total-data-files') AS BIGINT)                    AS total_data_files,
        CAST(element_at(summary, 'total-files-size') AS BIGINT)                    AS total_files_size_bytes
    FROM iceberg.demo."transactions$snapshots"

    UNION ALL

    SELECT
        'users'                                                                     AS table_name,
        committed_at,
        CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records,
        CAST(element_at(summary, 'added-data-files') AS BIGINT)                    AS added_data_files,
        CAST(element_at(summary, 'added-files-size') AS BIGINT)                    AS added_files_size_bytes,
        CAST(element_at(summary, 'total-records')    AS BIGINT)                    AS total_records,
        CAST(element_at(summary, 'total-data-files') AS BIGINT)                    AS total_data_files,
        CAST(element_at(summary, 'total-files-size') AS BIGINT)                    AS total_files_size_bytes
    FROM iceberg.demo."users$snapshots"
),
daily AS (
    SELECT
        table_name,
        date_trunc('day', committed_at)                                             AS day,
        count(*)                                                                    AS commit_count,
        coalesce(sum(added_records), 0)                                             AS total_added_records,
        coalesce(sum(added_data_files), 0)                                          AS total_added_files,
        coalesce(sum(added_files_size_bytes), 0)                                    AS total_added_bytes,
        -- avg size of newly written files on this day
        CASE WHEN sum(added_data_files) > 0
             THEN round(CAST(sum(added_files_size_bytes) AS DOUBLE) / sum(added_data_files) / 1024.0, 2)
             ELSE NULL
        END                                                                         AS avg_added_file_size_kb,
        -- end-of-day overall avg file size (last snapshot of the day)
        CAST(max_by(total_files_size_bytes, committed_at) AS DOUBLE)                AS eod_total_size_bytes,
        CAST(max_by(total_data_files, committed_at) AS BIGINT)                      AS eod_total_files,
        max_by(total_records, committed_at)                                         AS eod_total_records
    FROM snapshots
    GROUP BY 1, 2
)

SELECT
    table_name,
    day,
    commit_count,
    total_added_records,
    total_added_files,
    round(total_added_bytes / 1024.0 / 1024.0, 3)                                 AS total_added_mb,
    avg_added_file_size_kb,
    CASE WHEN eod_total_files > 0
         THEN round(eod_total_size_bytes / eod_total_files / 1024.0, 2)
         ELSE NULL END                                                              AS eod_avg_file_size_kb,
    eod_total_records,
    eod_total_files
FROM daily
ORDER BY table_name, day