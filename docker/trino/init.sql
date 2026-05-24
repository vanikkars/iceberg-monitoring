-- Pre-create the partition-transform tables that Flink SQL cannot express.
-- Flink 2.0's SQL parser only accepts column names in PARTITIONED BY; transform
-- functions like bucket() and day() cause ParseException at submission time.
-- Trino's Iceberg connector accepts the full partition spec via the
-- partitioning property.  Flink's CREATE TABLE IF NOT EXISTS then finds the
-- tables already in the catalog and skips recreation, but still writes to
-- them honouring the partition spec stored in the Iceberg metadata.

CREATE SCHEMA IF NOT EXISTS iceberg.demo;

-- bucket(user_id, 32) — 32 even-sized buckets on user_id
CREATE TABLE IF NOT EXISTS iceberg.demo.transactions_by_user_32 (
    transaction_id  VARCHAR,
    user_id         VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    "type"          VARCHAR,
    status          VARCHAR,
    event_time      TIMESTAMP(6)
) WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['bucket(user_id, 32)']
);

-- bucket(user_id, 4) — 4 coarser buckets on user_id
CREATE TABLE IF NOT EXISTS iceberg.demo.transactions_by_user_4 (
    transaction_id  VARCHAR,
    user_id         VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    "type"          VARCHAR,
    status          VARCHAR,
    event_time      TIMESTAMP(6)
) WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['bucket(user_id, 4)']
);

-- day(event_time) — one partition per calendar day
CREATE TABLE IF NOT EXISTS iceberg.demo.transactions_by_date (
    transaction_id  VARCHAR,
    user_id         VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    "type"          VARCHAR,
    status          VARCHAR,
    event_time      TIMESTAMP(6)
) WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(event_time)']
);

-- identity(event_minute) — one partition per minute (string column)
CREATE TABLE IF NOT EXISTS iceberg.demo.transactions_by_minute (
    transaction_id  VARCHAR,
    user_id         VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    "type"          VARCHAR,
    status          VARCHAR,
    event_time      TIMESTAMP(6),
    event_minute    VARCHAR
) WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['event_minute']
);
