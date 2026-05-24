-- ──────────────────────────────────────────────────────────────────────────
-- Flink SQL: Kafka → Iceberg  (users, transactions, orders topics)
-- ──────────────────────────────────────────────────────────────────────────

-- 0. Enable checkpointing every 30 seconds (required for Iceberg sink to commit data)
SET 'execution.checkpointing.interval' = '30s';

-- 1. Register the Iceberg catalog via Polaris's Iceberg REST endpoint.
--    Flink and Trino both connect to this same REST API (OAuth2 client credentials).
CREATE CATALOG iceberg_catalog WITH (
    'type'         = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri'          = 'http://polaris:8181/api/catalog',
    'warehouse'    = 'demo_lh',
    'credential'   = 'root:s3cr3t',
    'scope'        = 'PRINCIPAL_ROLE:ALL',
    -- Disable credential vending: Polaris defaults to STS AssumeRole which fails against
    -- MinIO (MinIO rejects the KMS policy Polaris includes in the policy document).
    -- Setting this to any non-recognized value produces an empty delegation set so Flink
    -- uses the static MinIO credentials below directly, bypassing STS entirely.
    'rest.access-delegation' = 'none',
    'io-impl'                = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'            = 'http://minio:9000',
    's3.path-style-access'   = 'true',
    's3.access-key-id'       = 'minioadmin',
    's3.secret-access-key'   = 'minioadmin'
);

-- 2. Create the target database in the Iceberg catalog
CREATE DATABASE IF NOT EXISTS iceberg_catalog.demo;

-- ── USERS ─────────────────────────────────────────────────────────────────

-- 3a. Kafka source: users
CREATE TEMPORARY TABLE kafka_users (
    user_id     STRING,
    name        STRING,
    email       STRING,
    country     STRING,
    created_at  TIMESTAMP(3),
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'users',
    'properties.bootstrap.servers'   = 'kafka:29092',
    'properties.group.id'            = 'flink-iceberg-users',
    'scan.startup.mode'              = 'earliest-offset',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 3b. Iceberg sink: users
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.users (
    user_id     STRING,
    name        STRING,
    email       STRING,
    country     STRING,
    created_at  TIMESTAMP(3)
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 3c. Streaming insert: users
INSERT INTO iceberg_catalog.demo.users
SELECT user_id, name, email, country, created_at
FROM kafka_users;

-- ── TRANSACTIONS ──────────────────────────────────────────────────────────

-- 4a. Kafka source: transactions (no partitions)
--     user_id is guaranteed non-null by the producer (seeded before any transaction)
CREATE TEMPORARY TABLE kafka_transactions (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'transactions',
    'properties.bootstrap.servers'   = 'kafka:29092',
    'properties.group.id'            = 'flink-iceberg-transactions',
    'scan.startup.mode'              = 'earliest-offset',
    'format'                         = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 4b. Iceberg sink: transactions (no partitions)
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.transactions (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3)
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 4c. Streaming insert: transactions (only rows with a non-null user_id)
INSERT INTO iceberg_catalog.demo.transactions
SELECT transaction_id, user_id, amount, currency, `type`, status, event_time
FROM kafka_transactions
WHERE user_id IS NOT NULL;

-- ── TRANSACTIONS BY USER BUCKET ──────────────────────────────────────────────

-- 5a. Iceberg sink: transactions partitioned into 32 buckets by user_id.
--     Table is pre-created by trino-init with bucket(user_id, 32) spec.
--     PARTITIONED BY is omitted here: Flink 2.0's parser rejects bucket()
--     transform syntax.  IF NOT EXISTS means Flink skips recreation and uses
--     the partition spec already stored in the Iceberg catalog metadata.
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.transactions_by_user_32 (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3)
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 5b. Streaming insert: transactions_by_user_32
INSERT INTO iceberg_catalog.demo.transactions_by_user_32
SELECT transaction_id, user_id, amount, currency, `type`, status, event_time
FROM kafka_transactions
WHERE user_id IS NOT NULL;


-- ── TRANSACTIONS BY USER BUCKET (4) ─────────────────────────────────────────

-- 6a. Iceberg sink: same bucket-by-user scheme but 4 buckets.
--     Pre-created by trino-init; PARTITIONED BY omitted for same reason as above.
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.transactions_by_user_4 (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3)
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 6b. Streaming insert: transactions_by_user_4
INSERT INTO iceberg_catalog.demo.transactions_by_user_4
SELECT transaction_id, user_id, amount, currency, `type`, status, event_time
FROM kafka_transactions
WHERE user_id IS NOT NULL;


-- ── TRANSACTIONS BY MINUTE ───────────────────────────────────────────────────

-- 7a. Iceberg sink: transactions partitioned by truncated event minute.
--     Iceberg has no native minute transform, so we materialise event_minute as
--     a STRING column ('yyyy-MM-dd HH:mm') and use identity partitioning on it.
--     Pre-created by trino-init; PARTITIONED BY omitted here for consistency.
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.transactions_by_minute (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3),
    event_minute    STRING
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 7b. Streaming insert: transactions_by_minute
INSERT INTO iceberg_catalog.demo.transactions_by_minute
SELECT
    transaction_id, user_id, amount, currency, `type`, status, event_time,
    DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm') AS event_minute
FROM kafka_transactions
WHERE user_id IS NOT NULL;

-- ── TRANSACTIONS BY DATE ────────────────────────────────

-- 8a. Iceberg sink: partitioned by calendar day of event_time.
--     Pre-created by trino-init with day(event_time) spec; PARTITIONED BY
--     omitted because Flink 2.0's parser also rejects day() as a transform.
CREATE TABLE IF NOT EXISTS iceberg_catalog.demo.transactions_by_date (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    currency        STRING,
    `type`          STRING,
    status          STRING,
    event_time      TIMESTAMP(3)
) WITH (
    'write.format.default'                   = 'parquet',
    'write.upsert.enabled'                   = 'false',
    'write.metadata.previous-versions-max'   = '2147483647'
);

-- 8b. Streaming insert: transactions_by_date
INSERT INTO iceberg_catalog.demo.transactions_by_date
SELECT transaction_id, user_id, amount, currency, `type`, status, event_time
FROM kafka_transactions
WHERE user_id IS NOT NULL;
