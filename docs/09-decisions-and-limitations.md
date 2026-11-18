# 09 — Decisions & Limitations

Short decision records: what was chosen, why, and what production would do differently.

## D1 — Redpanda instead of Apache Kafka
Single binary, Kafka-API compatible, no ZooKeeper/KRaft config, ~300 MB RAM idle.
Every client here (confluent-kafka, Kafka Connect) works unchanged.
**Prod:** Amazon MSK (or Redpanda/Confluent Cloud).

## D2 — Kafka Connect S3 Sink as the bronze writer (vs Logstash / Spark-on-Kafka)
The industry-standard way to land Kafka into S3. It gives exactly-once object
naming, size- or time-based rotation, and Hive-style time partitioning out of the
box, and it's operated through a REST API. Logstash would also work but is
heavier, and its S3 output doesn't offer offset-based exactly-once.
**Prod:** MSK Connect with the same connector, or Kinesis Data Firehose.

## D3 — Bronze as gzip JSON (not Parquet/Avro)
Bronze is meant to be the raw, unmodified record. JSON keeps it inspectable
(`mc cat | gunzip`) and needs no Schema Registry. Columnar storage starts at silver.
**Prod:** often Avro + Schema Registry for schema evolution guarantees.

## D4 — Spark file-source streaming with `availableNow` (Glue job style)
It gives incremental, checkpointed processing ("job bookmarks") with batch-job
economics, and the same code runs continuously with `--mode stream`.

## D5 — Delta Lake for silver/gold
ACID appends, `MERGE` for the gold upsert, time travel, `OPTIMIZE`/`VACUUM`, and
native Trino support. Iceberg would be an equally valid choice, and is the
native format for Athena/Glue in AWS.

## D6 — Redis written from gold (not from raw events)
The cache equals the curated view (max score, total hits), and it can be rebuilt
from gold at any time. Plain string keys with JSON values keep the hot path to a
single `GET`.

## D7 — MinIO via `bitnamilegacy/minio`
Upstream stopped publishing `minio/minio` images in 2025, and the old tags
aren't pullable anymore. The Bitnami legacy build (RELEASE 2025-04-22) is frozen
but works. Alternatives if it disappears: build MinIO from source, SeaweedFS, or Garage.

## D8 — Trino file metastore
It avoids a Hive Metastore + PostgreSQL (two more containers, ~1 GB). Metadata
lives in the lake bucket itself.
**Prod:** AWS Glue Data Catalog (`hive.metastore=glue`).

## D9 — Spark checkpoint on a local Docker volume
S3A has no atomic rename, which streaming checkpoints rely on. A local volume
avoids that class of issues in the lab.
**Prod:** checkpoints on HDFS/EFS, or S3 with a rename-safe committer, or Glue
bookmarks in a real Glue job.

## Known limitations

- Single node everywhere: no replication (topic RF=1, single MinIO drive), no HA.
- Credentials are in `.env` in plain text, with no TLS or auth on Redis/Kafka/Trino.
  Never expose these ports beyond localhost.
- End-to-end freshness of Redis is minutes (rotation + ETL schedule). For
  sub-second blocking of brand-new indicators, production adds a direct
  streaming path (e.g. Flink/Kafka Streams → Redis) next to this batch path. That's
  a lambda/kappa trade-off, and it's left out here on purpose.
- Gold `hits` can double-count if a batch is replayed after its MERGE committed
  (see [06](06-spark-etl.md)).
- Cross-batch duplicates in silver aren't removed.
