# Iceberg Broken Metadata — Causes & Fixes

Context: 150-topic lakehouse on AWS (Managed Flink + MSK), 1-minute Flink commit interval, no snapshot expiration configured, external compaction job triggered periodically.

---

## How Iceberg table metadata gets broken

### 1. Metadata file size explosion (most likely given this setup)

Every Flink checkpoint = 1 new snapshot entry appended to `metadata.json`. With no expiration:

```
1 commit/min × 60 min × 24 hr × 365 days = ~525,000 snapshots/year
```

The `metadata.json` file stores all snapshot references. It can grow to hundreds of MBs or GBs. At some point:
- S3 PUT/GET timeouts on reads/writes of this file
- The Flink committer OOMs trying to deserialize it
- The catalog (Glue or otherwise) hits size limits on stored properties

---

### 2. Compaction job conflicting with Flink writes

Iceberg uses **optimistic concurrency** — both Flink and the compaction job read the current metadata, make changes, then try to swap in a new `metadata.json`. If they race:

- Compaction rewrites data files and commits a new snapshot
- Flink simultaneously tries to commit its own snapshot based on the old metadata
- One commit wins; the other either retries or fails
- If the compaction job doesn't handle `CommitFailedException` correctly, it can leave a partial metadata state — new manifest files written to S3 but not properly registered, or the metadata pointer updated to a file that references data files already moved/deleted by compaction

---

### 3. Manifest file accumulation

Each snapshot creates at least one new manifest file. Without `rewrite_manifests`, you can end up with:
- Manifest lists pointing to tens of thousands of small manifest files
- Reading table state requires fetching all of them — causes timeouts in the Flink writer during the next commit

---

### 4. Compaction deleting files still referenced by live snapshots

If the compaction job runs `expire_snapshots` incorrectly or the retention window is too short, it can delete data/manifest files that are still referenced by snapshots within the retention window. Readers then get `FileNotFoundException` when trying to scan those snapshots.

---

### 5. S3 rate limiting / partial write during commit

The Flink Iceberg sink writes in this order:
1. Write data files to S3
2. Write manifest files to S3
3. Write new `metadata.json` to S3
4. Update catalog pointer to new `metadata.json`

If step 3 or 4 fails (S3 throttle, transient error), you get orphaned files on S3 but the catalog still points to the old valid metadata. Usually self-healing on retry — but if the job crashes mid-commit and the catalog pointer was partially updated, the table can appear broken.

---

### 6. AWS Glue catalog limits

See the dedicated section below for a full breakdown of Glue limits and what actually breaks at scale.

---

## Diagnosis

```bash
# Key questions based on the error:
# 1. FileNotFoundException      → compaction deleted referenced files
# 2. Timeout / OOM              → metadata too large
# 3. "Invalid metadata" / JSON  → partial write
# 4. Catalog error              → Glue property overflow

# Check raw metadata on S3
aws s3 ls s3://<bucket>/<table-prefix>/metadata/ --recursive | wc -l
aws s3 ls s3://<bucket>/<table-prefix>/metadata/ --recursive | sort | tail -5

# Check size of the latest metadata.json
aws s3api head-object --bucket <bucket> --key <path-to-latest-metadata.json>
```

---

## Fixes

### If the table is still readable (proactive repair)

Run via Spark (EMR or Glue job):

```python
# 1. Expire old snapshots — keep only last N hours
spark.sql("""
  CALL glue_catalog.system.expire_snapshots(
    table => 'db.table',
    older_than => TIMESTAMP '2025-01-01 00:00:00',
    retain_last => 100
  )
""")

# 2. Rewrite manifests — consolidates thousands of small manifests
spark.sql("""
  CALL glue_catalog.system.rewrite_manifests('db.table')
""")

# 3. Remove orphaned files (after expiry)
spark.sql("""
  CALL glue_catalog.system.remove_orphan_files(
    table => 'db.table',
    older_than => TIMESTAMP '2025-01-01 00:00:00'
  )
""")
```

### If the table is unreadable — manual metadata pointer reset

Find the last known-good `metadata.json` on S3:

```bash
# List all metadata files sorted by time
aws s3 ls s3://<bucket>/<prefix>/metadata/ | grep '.metadata.json' | sort

# Pick the latest valid one, then update the catalog pointer
```

For Glue catalog:
```bash
aws glue update-table --database-name <db> --table-input '{
  "Name": "<table>",
  "Parameters": {
    "metadata_location": "s3://<bucket>/<prefix>/metadata/<valid-file>.metadata.json"
  }
}'
```

For an Iceberg REST catalog, update the pointer to the last valid metadata file in the catalog's backing store.

### If data files were deleted by compaction prematurely

This is the hardest case. Options:
- If S3 Versioning is enabled, restore the deleted files
- If not, rebuild the table from the Kafka source (re-replay from MSK)

---

## Immediate remediation across all 150 tables

```python
# Set snapshot expiration going forward
spark.sql("""
  ALTER TABLE glue_catalog.db.mytable
  SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '604800000',  -- 7 days
    'history.expire.min-snapshots-to-keep' = '10'
  )
""")
```

Run `expire_snapshots` + `rewrite_manifests` on all 150 tables — ideally during a low-traffic window with Flink paused, to avoid conflicting with the compaction job.

---

## `metadata_location` — deep dive

### What it is

`metadata_location` is a single key-value pair stored in the **catalog** (e.g., Glue table parameters). It's just a pointer — a short S3 path — telling any reader "this is the current authoritative state of the table."

```
metadata_location = s3://my-lakehouse/warehouse/demo/transactions/metadata/00042-3f8a1b2c.metadata.json
```

The naming convention:
- `00042` — monotonically increasing version number, incremented on every commit
- UUID — unique identifier for that specific write

---

### The commit cycle — how the pointer moves

Every Flink checkpoint triggers this sequence:

```
Commit N-1 (current state in Glue):
  metadata_location → s3://.../metadata/00041-aaa.metadata.json

Flink commits checkpoint:
  1. Writes data files   → s3://.../data/00001-abc.parquet
  2. Writes manifest     → s3://.../metadata/abc-m0.avro
  3. Writes new metadata → s3://.../metadata/00042-bbb.metadata.json
  4. UpdateTable in Glue → metadata_location = s3://.../metadata/00042-bbb.metadata.json

Commit N (new state in Glue):
  metadata_location → s3://.../metadata/00042-bbb.metadata.json
```

The old `00041-aaa.metadata.json` stays on S3 indefinitely — nothing deletes it automatically.

---

### What's inside a `metadata.json`

```json
{
  "format-version": 2,
  "table-uuid": "3f8a1b2c-...",
  "location": "s3://my-lakehouse/warehouse/demo/transactions",
  "current-snapshot-id": 8839203741,
  "last-sequence-number": 42,

  "snapshots": [
    {
      "snapshot-id": 8839203741,
      "parent-snapshot-id": 7712938420,
      "timestamp-ms": 1715000000000,
      "manifest-list": "s3://.../metadata/snap-8839203741-1.avro",
      "summary": { "operation": "append", "added-records": "1240" }
    },
    {
      "snapshot-id": 7712938420,
      ...
    }
    // ... 524,998 more entries after a year of 1-min commits
  ],

  "metadata-log": [
    { "timestamp-ms": 1714999940000, "metadata-file": "s3://.../metadata/00041-aaa.metadata.json" },
    { "timestamp-ms": 1714999880000, "metadata-file": "s3://.../metadata/00040-zzz.metadata.json" }
    // grows unboundedly too
  ],

  "schemas": [...],
  "partition-specs": [...],
  "current-schema-id": 0
}
```

The `snapshots` array and `metadata-log` array are what grow unboundedly — **they live inside this file on S3**, not in Glue. After a year of 1-min commits, this file can easily be 500 MB+.

---

## AWS Glue limits — what actually breaks at scale

### What Glue stores for an Iceberg table

The table parameters stored in Glue are minimal:

```
table_type                 = ICEBERG
metadata_location          = s3://.../.../00042-bbb.metadata.json
previous_metadata_location = s3://.../.../00041-aaa.metadata.json  (some catalogs)
```

`metadata_location` is just a short S3 path — nowhere near any size limit on its own. The bloated `metadata.json` lives on S3, not in Glue.

### The real Glue bottleneck: `UpdateTable` API rate limit

Glue has a default soft limit of **10 write calls/second per account** across all tables.

```
150 tables × 1 commit/min = 2.5 UpdateTable calls/sec (average)
```

That looks fine in aggregate, but Flink checkpoints are typically **synchronized** — all 150 jobs checkpoint at the same wall-clock time, producing a burst of **150 `UpdateTable` calls at once**. Glue returns `ThrottlingException`, Flink retries with backoff, commit latency spikes, and in the worst case the job fails or falls behind.

Same issue applies on the read side: `GetTable` is also rate-limited at ~10 calls/sec. Every Athena query, EMR job, or compaction job planning phase hits this.

### The actual Glue table parameter size limit

AWS Glue enforces a hard limit of **~512 KB total for all table parameters combined**. For standard Iceberg this is unlikely to be hit from `metadata_location` alone, but some catalog implementations or third-party tools store additional state (statistics, column metrics, etc.) in Glue parameters, which can push toward the limit over time.

### What actually breaks from large `metadata.json` (S3, not Glue)

The real size problem is on S3 and in the Flink/Spark JVM:

| Symptom | Root cause |
|---|---|
| Flink committer OOM | Deserializing a 500 MB JSON into heap during commit |
| Slow Athena / EMR query planning | Reading 500 MB metadata.json before touching any data |
| S3 read timeout during commit | GET on a large object exceeds SDK timeout |
| Compaction job hangs at planning | Loading all 500k snapshot entries into Spark driver memory |

The fix in all cases is `expire_snapshots` — it produces a new small `metadata.json` that replaces the bloated one and truncates both the `snapshots` array and `metadata-log`.