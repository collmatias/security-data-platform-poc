# 07 — Trino / Threat Hunting (Athena simulation)

## Catalogs

| Catalog | File | Connector | Reads |
|---------|------|-----------|-------|
| `hive` | `trino/catalog/hive.properties` | Hive (JSON) | Bronze raw files |
| `delta` | `trino/catalog/delta.properties` | Delta Lake | Silver and Gold |

Both use:

- **File metastore** (`hive.metastore=file`) stored in MinIO under
  `s3://datalake/_trino_metastore/…`. It plays the role of the Glue Data Catalog
  without a Hive Metastore + database container. It's for labs only (no concurrency guarantees).
- **Native S3 filesystem** (`fs.native-s3.enabled=true`) pointed at
  `http://minio:9000` with path-style access. Credentials come from env via
  `${ENV:AWS_ACCESS_KEY_ID}`.

## Table registration

Tables can only be registered after the data exists, so registration is a
step you run, not a startup hook:

```bash
docker compose exec trino bash /opt/scripts/register-tables.sh
```

| Table | How it's registered | Re-run needed when |
|-------|--------------------|--------------------|
| `hive.bronze.security_events_raw` | `CREATE TABLE … WITH (external_location, format='JSON', partitioned_by=ARRAY['dt','hr'])` | A new hour arrives: `sync_partition_metadata` discovers new `dt=/hr=` folders |
| `delta.lake.security_events` | `CALL delta.system.register_table(…)` | Never. The schema and files are read from the Delta log on every query. |
| `delta.lake.threat_indicators` | same | Never |

`register_table` fails if the table already exists, so the script checks
`information_schema` first. That makes the script safe to re-run.

## Running queries manually

The Trino CLI is bundled in the `trino` container (`trino --version` → 470). You
don't need to download the CLI jar or install Java on the host.

### Interactive shell

```bash
docker compose exec trino trino --catalog delta --schema lake
```

```sql
SHOW CATALOGS;                 -- delta, hive, system
SHOW TABLES;                   -- security_events, threat_indicators
DESCRIBE security_events;

SELECT indicator, hits, max_threat_score
FROM threat_indicators
WHERE indicator_type = 'ip'
ORDER BY hits DESC
LIMIT 10;

-- Other catalogs need fully qualified names
SELECT count(*) FROM hive.bronze.security_events_raw;
```

Exit with `quit;` or `Ctrl+D`. Without `--catalog/--schema`, always use
`catalog.schema.table` (e.g. `delta.lake.security_events`).

### One-off query

```bash
docker compose exec trino trino --execute "SELECT count(*) FROM delta.lake.security_events"
```

### Run a `.sql` file

`trino/sql/` is mounted at `/opt/sql` inside the container, so put files there:

```bash
docker compose exec trino trino -f /opt/sql/02_threat_hunting.sql
```

### Output formats / export

`--output-format` accepts `ALIGNED` (readable table), `CSV`, `CSV_HEADER`,
`TSV`, `TSV_HEADER`, `JSON`, and others. Use `-T` (no TTY) when redirecting to a file:

```bash
docker compose exec -T trino trino --output-format CSV_HEADER \
  --execute "SELECT * FROM delta.lake.threat_indicators" > indicators.csv
```

### Other clients

| Client | Connection | Notes |
|--------|------------|-------|
| DBeaver / DataGrip | JDBC `jdbc:trino://localhost:8080`, any user, no password | The closest thing to the Athena console: editor, autocomplete, result grid |
| Python | `pip install trino` → `trino.dbapi.connect(host="localhost", port=8080, user="me", catalog="delta", schema="lake")` | Notebooks, scripts, pandas |
| Host CLI jar | `java -jar trino-cli-470-executable.jar --server localhost:8080` | Needs a local Java runtime. Not needed here because the container CLI is equivalent. |
| Web UI | http://localhost:8080 | Monitoring only (running queries, stages, plans), not a SQL editor |

## Sample queries

All in [`trino/sql/02_threat_hunting.sql`](../trino/sql/02_threat_hunting.sql):

```bash
docker compose exec trino trino -f /opt/sql/02_threat_hunting.sql
```

1. Daily volume and % malicious per event type
2. Top 10 most active malicious IPs (gold)
3. Internal users most targeted by phishing
4. Malicious events by action (detection-gap analysis)
5. Bronze vs silver row-count reconciliation (data-quality check)

More hunting ideas:

```sql
-- Pivot: everything a malicious IP touched in the last hour
SELECT event_ts, event_type, domain, recipient, action
FROM delta.lake.security_events
WHERE source_ip = '185.164.238.114'
  AND event_date = cast(current_date AS varchar)
  AND event_ts > current_timestamp - INTERVAL '1' HOUR
ORDER BY event_ts;

-- Lookalike domains (brand impersonation)
SELECT indicator, hits, max_threat_score
FROM delta.lake.threat_indicators
WHERE indicator_type = 'domain' AND regexp_like(indicator, '(paypa1|micros0ft|app1e|amaz0n|g00gle)');

-- Time travel: how many indicators did gold have 3 versions ago?
SELECT count(*) FROM delta.lake."threat_indicators$history";
```

Always filter on `event_date` (silver) or `dt`/`hr` (bronze). These are
partition columns, so Trino prunes files exactly like Athena does, and the
same habit saves money on Athena's $/TB scanned.
