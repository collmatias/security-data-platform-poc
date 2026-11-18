# 05 — Bronze Layer: Kafka Connect S3 Sink

Config: [`kafka-connect/connectors/s3-sink-bronze.json`](../kafka-connect/connectors/s3-sink-bronze.json)
Plugin: Confluent `kafka-connect-s3` 10.5.17, baked into the image (no runtime download).

## The small-file problem it solves

When Spark Structured Streaming reads Kafka directly, every micro-batch commits
new Parquet files (one or more per partition per trigger). At a 10 s trigger
that's thousands of tiny files per day per partition. That means slow S3 listings,
slow Trino planning, and expensive compaction. Here Connect absorbs the stream
and writes **few large objects**, and Spark processes those in bulk.

Measured in the lab: **100,000 events → 6 bronze files** (3 partitions × 2 rotations).

## Rotation (when a file is closed and uploaded)

A file per Kafka partition is open at any time. It's committed to S3 when the
**first** of these triggers fires:

| Setting | Lab value | Meaning |
|---------|-----------|---------|
| `flush.size` | 50,000 | records per file (size-based rotation) |
| `rotate.interval.ms` | 60,000 | span of *record timestamps* in the file |
| `rotate.schedule.interval.ms` | 60,000 | *wall-clock* rotation, so files close even when traffic stops |
| `partition.duration.ms` + `path.format` | 1 h, `dt=/hr=` | a new hour always starts a new file |

Production values are usually `flush.size` sized for ~128–256 MB files and
5–15 min intervals. The lab uses 60 s so you see results quickly.

Observed latency: the last records of a burst show up in bronze about
60–90 s after they were produced.

## Other settings worth knowing

| Setting | Why |
|---------|-----|
| `format.class = JsonFormat` + `s3.compression.type = gzip` | Raw, human-readable, ~10× smaller. Spark/Trino decompress by `.gz` extension. |
| `value.converter = JsonConverter`, `schemas.enable=false` | Producer sends schemaless JSON |
| `transforms.kafkaMeta` (`InsertField$Value`) | Adds `kafka_partition` / `kafka_offset` to each record |
| `timestamp.extractor = Record` | Partitions by Kafka record time, which is reproducible on replay |
| `store.url`, `s3.path.style.access.enabled` | Point the AWS SDK at MinIO |
| `errors.tolerance = all` + `errors.log.enable` | A poison message is logged and skipped instead of stopping the task. In production, add a DLQ (sink connectors support `errors.deadletterqueue.topic.name`). |
| `tasks.max = 3` | One task per topic partition |

Credentials come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` on the worker
container, never from the connector JSON (connector configs are readable via REST).

## Delivery semantics

The S3 sink is **exactly-once into S3** for a deterministic partitioner: object
names are `<topic>+<partition>+<startOffset>`, and Connect only commits Kafka
offsets after the upload. So a crash causes a re-upload that overwrites the
same key instead of creating a duplicate.

## Operating it

```bash
curl -s localhost:8083/connectors/s3-sink-bronze/status | jq     # health
curl -s -X POST localhost:8083/connectors/s3-sink-bronze/restart?includeTasks=true
curl -s -X PUT localhost:8083/connectors/s3-sink-bronze/pause
curl -s -X PUT localhost:8083/connectors/s3-sink-bronze/resume
docker compose exec redpanda rpk group describe connect-s3-sink-bronze   # consumer lag
```

To change the config, edit the JSON and run `docker compose up connect-init`
(it's an upsert).
