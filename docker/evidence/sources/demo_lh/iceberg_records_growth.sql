-- Total records and data files at each snapshot commit — shows pipeline write rate over time.
SELECT
    'transactions'                                                              AS table_name,
    committed_at,
    CAST(element_at(summary, 'total-records')    AS BIGINT)                    AS total_records,
    CAST(element_at(summary, 'total-data-files') AS BIGINT)                    AS total_data_files,
    CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records
FROM iceberg.demo."transactions$snapshots"

UNION ALL

SELECT
    'users'                                                                     AS table_name,
    committed_at,
    CAST(element_at(summary, 'total-records')    AS BIGINT)                    AS total_records,
    CAST(element_at(summary, 'total-data-files') AS BIGINT)                    AS total_data_files,
    CAST(element_at(summary, 'added-records')    AS BIGINT)                    AS added_records
FROM iceberg.demo."users$snapshots"

ORDER BY table_name, committed_at