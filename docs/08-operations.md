# 08 — Operations & Troubleshooting

## Endpoints

| What | URL / address | Credentials |
|------|---------------|-------------|
| Kafka API (host) | `localhost:19092` | — |
| Kafka API (containers) | `redpanda:9092` | — |
| Redpanda admin | http://localhost:9644 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| MinIO S3 API | http://localhost:9000 | `.env` |
| MinIO console | http://localhost:9001 | `.env` |
| Redis | `localhost:6379` | — |
| Spark UI (only while a job runs) | http://localhost:4040 | — |
| Trino UI | http://localhost:8080 | any user name |

## Health at a glance

```bash
docker compose ps -a                                         # healthy / Exited (0) for *-init
docker stats --no-stream                                     # usage vs limits
curl -s localhost:8083/connectors/s3-sink-bronze/status      # connector + tasks RUNNING
docker compose exec redpanda rpk cluster health
docker compose exec redpanda rpk group list                  # consumer groups (lag via `rpk group describe`)
```

## Logs

```bash
docker compose logs -f kafka-connect | grep -i "Files committed"   # each bronze upload
docker compose logs redpanda-init minio-init connect-init          # bootstrap steps
docker compose logs trino | tail
```

## Common problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| `pull access denied for minio/minio` | MinIO stopped publishing images on Docker Hub (2025) | Already handled: the stack uses `bitnamilegacy/minio` |
| Topic has 1 partition | The connector subscribed before the topic existed, so it was auto-created | `redpanda-init` creates it first. If it happened anyway: `docker compose down -v` and start again |
| ETL says "No bronze data … yet" | Connector hasn't rotated a file yet | Wait 60–90 s after producing, then check `mc ls` |
| ETL processes 0 events | Checkpoint already contains every file | Expected. Produce more, or see "Re-processing" in [06](06-spark-etl.md) |
| Fewer events in bronze than produced | Tail of the stream is still buffered in Connect | Wait for the next rotation (≤ 90 s) |
| `Table already exists` from Trino | Called `register_table` twice by hand | Use `scripts/register-tables.sh` (idempotent) |
| Bronze counts lag in Trino after a new hour | New `hr=` partition not registered | Re-run `register-tables.sh` |
| Container `OOMKilled` (`docker inspect <c> --format '{{.State.OOMKilled}}'`) | Limit too low for the workload | Raise the limit per [04](04-resources-and-tuning.md) |
| `ADD --chmod` build error | Old Docker without BuildKit | Docker ≥ 23 or `DOCKER_BUILDKIT=1` |
| A `timeout`-killed `spark-submit` keeps running | `docker compose exec` doesn't forward the kill | `docker compose exec spark pkill -f etl.py` |
| Port already in use | Another local service | Change the left side of `ports:` in compose |

## Reset levels

```bash
docker compose restart <service>      # bounce one component
docker compose down                   # stop all, keep volumes
docker compose down -v                # wipe ALL data (topics, lake, checkpoints, Redis)
docker compose build --no-cache       # rebuild images (e.g. after changing versions)
```
