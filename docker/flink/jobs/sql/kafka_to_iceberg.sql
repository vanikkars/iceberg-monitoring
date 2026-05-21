-- ──────────────────────────────────────────────────────────────────────────
-- Flink SQL: Kafka → Iceberg  (users, transactions, orders topics)
-- ──────────────────────────────────────────────────────────────────────────

-- 0. Enable checkpointing every 30 seconds (required for Iceberg sink to commit data)
SET 'execution.checkpointing.interval' = '30s';

-- 1. Register the Iceberg catalog via Polaris's Iceberg REST endpoint.
--    Flink, Trino, and DuckDB all connect to this same REST API (OAuth2 client credentials).
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

-- 4a. Kafka source: transactions
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

-- 4b. Iceberg sink: transactions
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
