# 02 — Quickstart / Runbook

Every command runs from the repository root.

## 0. Prerequisites (check once)

```bash
docker --version            # >= 24 (BuildKit is required for ADD --chmod)
docker compose version      # v2.x
docker context show         # which engine you are talking to (see below)
```

**Memory/CPU for the Docker engine.** The stack needs ~16 GiB of RAM at its
limits (~8 GiB in normal use) and up to ~16 CPUs at its limits.

- `docker context show` → `default` (native Docker Engine on Linux): **nothing to
  change.** Containers use host RAM/CPU directly, and the per-container limits in
  `docker-compose.yml` cap them.
- `docker context show` → `desktop-linux` (Docker Desktop): Desktop runs a VM
  with its own budget. Open *Settings → Resources* and set at least:
  **Memory 18 GB, CPUs 12, Swap 2 GB, Virtual disk 64 GB**, then *Apply & restart*.
  The defaults (~50% RAM, 2 GB swap) are too small for Spark + Trino + Connect at once.

Details in [04-resources-and-tuning.md](04-resources-and-tuning.md).

Free host ports needed: `4040 6379 8080 8083 9000 9001 9644 19092`.

## 1. Build and start the stack

```bash
docker compose build              # first time: ~2-4 min (downloads jars + S3 sink plugin)
docker compose up -d
docker compose ps -a              # wait until everything is "healthy" / "Exited (0)"
```

Three one-shot init containers run automatically and should end with `Exited (0)`:

| Init container | Does |
|----------------|------|
| `redpanda-init` | Creates topic `security-events` (3 partitions, 7-day retention) |
| `minio-init` | Creates bucket `datalake` |
| `connect-init` | Registers the `s3-sink-bronze` connector |

They're idempotent. To run the same steps by hand:

```bash
# Topic
docker compose exec redpanda rpk topic create security-events -p 3 -r 1
# Bucket
docker compose run --rm --entrypoint mc minio-init mb --ignore-existing lake/datalake
# Connector (create or update)
curl -X PUT -H "Content-Type: application/json" \
     --data @kafka-connect/connectors/s3-sink-bronze.json \
     http://localhost:8083/connectors/s3-sink-bronze/config
```

Verify:

```bash
docker compose exec redpanda rpk topic describe security-events -p
curl -s localhost:8083/connectors/s3-sink-bronze/status   # connector + 3 tasks RUNNING
```

## 2. Produce security events

```bash
# 100k events at 5k ev/s (runs in a container on the lab network)
docker compose run --rm producer --count 100000 --rate 5000

# Or forever at 500 ev/s, stop with Ctrl+C
docker compose run --rm producer --count 0 --rate 500
```

Optional, from the host instead: `pip install confluent-kafka==2.6.1 && python producer/producer.py --bootstrap localhost:19092`.

## 3. Wait for the bronze files (Kafka Connect)

Files are committed every **60 s** or every **50,000 records per partition**,
whichever comes first. Give it 1–2 minutes after producing, then:

```bash
docker compose run --rm --entrypoint mc minio-init ls -r lake/datalake/bronze
# security-events/dt=2026-11-18/hr=14/security-events+0+0000000000.json.gz ...
```

MinIO console: http://localhost:9001 (user/password from `.env`).

## 4. Run the Spark ETL (Glue-style job)

```bash
# Process every new bronze file, then exit (run as often as you like)
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/etl.py --mode batch
# -> WARNING etl - batch=0 events=100000 malicious_indicators=350
```

Continuous alternative (polls every 30 s). Stop it with `Ctrl+C` or
`docker compose exec spark pkill -f etl.py`:

```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/etl.py --mode stream --interval 30
```

Spark UI while a job runs: http://localhost:4040

## 5. Query Redis (detection-engine path)

```bash
# Pick any malicious IP / domain that was loaded
docker compose exec redis redis-cli --scan --pattern 'threat:ip:*' | head -3
docker compose exec redis redis-cli GET threat:ip:185.164.238.114
# {"score": 100, "category": "phishing", "hits": 14, "first_seen": "...", "last_seen": "..."}

docker compose exec redis redis-cli GET threat:domain:account-okta-sso812.top
docker compose exec redis redis-cli GET threat:ip:8.8.8.8          # (nil) -> not a known threat
docker compose exec redis redis-cli TTL threat:ip:185.164.238.114   # seconds until expiry (7 days)

# Latency check (~0.3 ms avg on a laptop)
docker compose exec redis redis-cli --latency     # Ctrl+C to stop
```

The exact IPs/domains are random but fixed by a seed. Use `--scan` to find real keys.

## 6. Query the lake with Trino (Athena path)

```bash
# Register bronze/silver/gold tables (idempotent; re-run to discover new hourly partitions)
docker compose exec trino bash /opt/scripts/register-tables.sh

# Run the sample threat-hunting queries
docker compose exec trino trino -f /opt/sql/02_threat_hunting.sql

# Interactive SQL shell
docker compose exec trino trino
```

```sql
SELECT indicator, max_threat_score, threat_category, hits
FROM delta.lake.threat_indicators
WHERE indicator_type = 'domain'
ORDER BY hits DESC LIMIT 5;
```

Trino UI: http://localhost:8080 (any user name, no password).

## 7. (Optional) Compact Delta tables

```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/maintenance.py
```

## 8. Stop / reset

```bash
docker compose down        # stop, keep data
docker compose down -v     # stop and DELETE all data (topics, lake, checkpoints, Redis)
```

After `down -v`, start again from step 1.
