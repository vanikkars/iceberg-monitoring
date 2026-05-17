-- Current data file distribution per table, from Iceberg $files metadata.
-- Surfaces small-file problems: if avg_file_size_kb is very low and file_count is high,
-- a RewriteDataFiles compaction job is recommended.
SELECT
    'transactions'                                                         AS table_name,
    count(*)                                                               AS file_count,
    sum(record_count)                                                      AS total_records,
    round(CAST(avg(record_count) AS DOUBLE), 0)                            AS avg_records_per_file,
    min(record_count)                                                      AS min_records_per_file,
    max(record_count)                                                      AS max_records_per_file,
    round(CAST(avg(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)             AS avg_file_size_kb,
    round(CAST(min(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)             AS min_file_size_kb,
    round(CAST(max(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)             AS max_file_size_kb,
    round(CAST(sum(file_size_in_bytes) AS DOUBLE) / 1024.0 / 1024.0, 2)    AS total_size_mb
FROM iceberg.demo."transactions$files"
WHERE content = 0  -- DATA files only (exclude delete files)

UNION ALL

SELECT
    'users'                                                                 AS table_name,
    count(*)                                                                AS file_count,
    sum(record_count)                                                       AS total_records,
    round(CAST(avg(record_count) AS DOUBLE), 0)                             AS avg_records_per_file,
    min(record_count)                                                       AS min_records_per_file,
    max(record_count)                                                       AS max_records_per_file,
    round(CAST(avg(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)              AS avg_file_size_kb,
    round(CAST(min(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)              AS min_file_size_kb,
    round(CAST(max(file_size_in_bytes) AS DOUBLE) / 1024.0, 2)              AS max_file_size_kb,
    round(CAST(sum(file_size_in_bytes) AS DOUBLE) / 1024.0 / 1024.0, 2)     AS total_size_mb
FROM iceberg.demo."users$files"
WHERE content = 0