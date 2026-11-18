# Security Data Platform PoC

Local, docker-compose based lab that simulates a security vendor's data platform:

```
producer ─► Redpanda (MSK) ─► Kafka Connect S3 Sink ─► MinIO bronze (raw JSON)
                                                          │
                                         Spark + Delta (Glue)
                                     ┌────────────┼─────────────┐
                                     ▼            ▼             ▼
                             silver events   gold indicators   Redis (ElastiCache)
                                     └──── Trino (Athena) ────┘     └─► <1 ms GET for the detection engine
```

## TL;DR

```bash
docker compose build && docker compose up -d
docker compose run --rm producer --count 100000 --rate 5000
# wait ~90 s for Kafka Connect to rotate files into bronze
docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/etl.py
docker compose exec redis redis-cli --scan --pattern 'threat:ip:*' | head -1 | xargs docker compose exec -T redis redis-cli GET
docker compose exec trino bash /opt/scripts/register-tables.sh
docker compose exec trino trino -f /opt/sql/02_threat_hunting.sql
```

Full documentation: **[docs/INDEX.md](docs/INDEX.md)**. The step-by-step runbook,
including Docker memory/CPU settings, is in [docs/02-quickstart.md](docs/02-quickstart.md).
