# Security Data Platform PoC — Documentation Index

Local lab that simulates the data platform of an email/web security vendor:
millions of events per day, a <10 ms threat-intel lookup path for the detection
engine, and an S3 data lake for threat-hunting analytics.

Start with **Quickstart** if you just want it running. Each other file covers one topic.

| # | Document | What it covers |
|---|----------|----------------|
| 01 | [Architecture](01-architecture.md) | Components, AWS equivalents, data flow, medallion layers |
| 02 | [Quickstart / Runbook](02-quickstart.md) | Step-by-step commands: start, produce, ETL, Redis GET, Trino SELECT |
| 03 | [Data Model](03-data-model.md) | Event schema, bronze path layout, silver/gold tables, Redis key design |
| 04 | [Resources & Tuning](04-resources-and-tuning.md) | CPU/RAM limits per container, Docker Engine / Desktop settings |
| 05 | [Bronze Layer (Kafka Connect)](05-bronze-kafka-connect.md) | S3 Sink buffering, small-file problem, rotation settings |
| 06 | [Spark ETL](06-spark-etl.md) | Incremental processing, idempotency, batch vs stream mode, maintenance |
| 07 | [Trino / Threat Hunting](07-trino-queries.md) | Catalogs, table registration, manual querying (CLI, DBeaver, Python), sample hunting queries |
| 08 | [Operations & Troubleshooting](08-operations.md) | UIs, health checks, logs, reset, common errors |
| 09 | [Decisions & Limitations](09-decisions-and-limitations.md) | Why each technology/config was chosen, what differs from production |

## Repository layout

```
.
├── docker-compose.yml          # Whole stack, resource limits, one-shot init containers
├── .env                        # Lab credentials / bucket / topic names
├── producer/                   # Mock security event generator (Python, confluent-kafka)
├── kafka-connect/
│   ├── Dockerfile              # cp-kafka-connect + Confluent S3 Sink plugin
│   └── connectors/s3-sink-bronze.json
├── spark/
│   ├── Dockerfile              # Spark 3.5 + hadoop-aws + Delta Lake + redis-py
│   ├── conf/spark-defaults.conf
│   └── jobs/
│       ├── etl.py              # Bronze -> Silver/Gold Delta + Redis
│       └── maintenance.py      # OPTIMIZE / VACUUM
├── trino/
│   ├── catalog/                # hive (bronze JSON) and delta (silver/gold) catalogs
│   └── sql/                    # Table registration + threat-hunting queries
├── scripts/
│   ├── register-connector.sh   # Upsert the S3 sink connector (used by connect-init)
│   └── register-tables.sh      # Idempotent Trino table registration
└── docs/                       # You are here
```
