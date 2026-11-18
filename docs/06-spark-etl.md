# 06 — Spark ETL (Glue simulation)

Job: [`spark/jobs/etl.py`](../spark/jobs/etl.py) · Runtime: Spark 3.5.3, Delta 3.2.1,
hadoop-aws 3.3.4, redis-py 5.0.8. All of it is baked into `secdata/spark:3.5.3-delta`.

## What one run does

```
bronze/*.json.gz (new files only)
   └─ transform(): parse event_time, normalize domain, is_malicious, event_date, lineage
        ├─ 1. Silver: Delta append, partitioned by event_date (idempotent per batch)
        ├─ 2. Gold:   aggregate malicious IPs/domains → MERGE into threat_indicators
        └─ 3. Redis:  read back the merged gold rows for touched indicators → SET … EX
```

## Incremental processing (the "job bookmark")

The job uses the Structured Streaming **file source** on the bronze prefix with
an explicit schema, `recursiveFileLookup` (walks `dt=/hr=`) and
`pathGlobFilter=*.json.gz`. The checkpoint at `/opt/spark/checkpoints/bronze_to_silver`
(Docker volume `spark-checkpoints`) records every file already processed, the
same role as Glue job bookmarks. Re-running with no new files processes nothing.

Bronze objects only appear once fully uploaded (S3 multipart completion is
atomic), so Spark never reads a half-written file.

## Modes

| Mode | Trigger | Use it like |
|------|---------|-------------|
| `--mode batch` (default) | `availableNow` | A scheduled Glue job (cron / Airflow / Step Functions): drains new files, exits |
| `--mode stream --interval N` | `processingTime N s` | Always-on job polling bronze every N seconds |

`--max-files-per-trigger` (default 200) caps micro-batch size, which bounds
memory on a backlog. Stop a stream run with `docker compose exec spark pkill -f etl.py`.

## Correctness and idempotency

| Concern | How it's handled |
|---------|------------------|
| Batch retried after a crash | Silver append uses Delta `txnAppId` + `txnVersion=batch_id`. A replayed batch id is skipped. |
| Duplicates inside a batch | `dropDuplicates(["event_id"])` |
| Duplicates across batches | Not removed (bronze is exactly-once per offset, so this only happens if the producer re-sends). Production would use `MERGE` on `event_id` or `foreachBatch` dedup with a watermark. |
| Gold replay | `hits += n` isn't idempotent if the same batch is replayed after the MERGE committed but before the checkpoint. Acceptable for counters. Use Delta CDF or a batch-id column if exact counts matter. |
| Redis consistency | Redis is written from gold *after* the MERGE, so it can be rebuilt from gold at any time |
| Bad records | Rows without `event_id` or with a score outside 0–100 are dropped in `transform()` |

Gotcha that's already fixed: inside `foreachBatch`, temp views and SQL must use
`batch_df.sparkSession` (a cloned session), not the outer `spark` object.
Otherwise `MERGE … USING batch_indicators` fails with `TABLE_OR_VIEW_NOT_FOUND`.

## Configuration

`spark/conf/spark-defaults.conf` holds Delta extensions, S3A→MinIO, timezone UTC,
`local[4]`, and driver memory. Job-level env vars (set in compose):

| Var | Default | |
|-----|---------|-|
| `THREAT_THRESHOLD` | 80 | `threat_score > X` ⇒ malicious |
| `REDIS_HOST` / `REDIS_PORT` | redis / 6379 | |
| `REDIS_TTL_SECONDS` | 604800 | 7 days |
| `CHECKPOINT_DIR` | /opt/spark/checkpoints/bronze_to_silver | |
| `LAKE_BUCKET` | s3a://datalake | |

## Maintenance (`maintenance.py`)

The Delta tables still gain one or more files per ETL run. `OPTIMIZE` bin-packs
them (in the lab: 8 files → 1), and `VACUUM` deletes unreferenced files older
than the retention period (default 168 h). Schedule it daily in production.

```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/maintenance.py
```

## Re-processing from scratch

Bronze is the replay source. To rebuild silver/gold/Redis:

```bash
docker compose run --rm --entrypoint mc minio-init rm -r --force lake/datalake/silver lake/datalake/gold
docker compose exec redis redis-cli --scan --pattern 'threat:*' | xargs -r docker compose exec -T redis redis-cli DEL
docker compose stop spark && docker compose rm -f spark && docker volume rm secdata-poc_spark-checkpoints
docker compose up -d spark
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/etl.py
```

After a rebuild, Trino's registered Delta tables still point to the same
locations, so no re-registration is needed.
