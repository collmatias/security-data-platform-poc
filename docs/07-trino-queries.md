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

### Exploring what already exists (metadata queries)

Every query below was tested against the running lab. Run them from the
interactive shell, or wrap them in `--execute "..."`.

```sql
-- Catalogs and schemas
SHOW CATALOGS;                                   -- delta, hive, system
SHOW SCHEMAS FROM delta;                         -- information_schema, lake
SHOW SCHEMAS FROM hive;                          -- bronze, ...

-- Tables in one schema
SHOW TABLES FROM delta.lake;                     -- security_events, threat_indicators
SHOW TABLES FROM hive.bronze;                    -- security_events_raw

-- Every lake table across both catalogs, in one result
SELECT table_cat, table_schem, table_name
FROM system.jdbc.tables
WHERE table_cat IN ('delta', 'hive') AND table_schem <> 'information_schema'
ORDER BY 1, 2, 3;

-- Same idea, standard SQL, one catalog at a time
SELECT table_schema, table_name
FROM delta.information_schema.tables
WHERE table_schema <> 'information_schema';

-- Structure of a table
DESCRIBE delta.lake.security_events;             -- alias: SHOW COLUMNS FROM ...
SHOW CREATE TABLE delta.lake.security_events;    -- DDL incl. S3 location and partitioning

-- Quick data preview
SELECT * FROM delta.lake.threat_indicators LIMIT 10;
```

Trino also exposes **metadata tables**. Append `$<name>` to the table name and
wrap it in double quotes:

```sql
-- Bronze: which dt/hr partitions are registered (re-run register-tables.sh if one is missing)
SELECT * FROM hive.bronze."security_events_raw$partitions";

-- Delta: commit history (CREATE / WRITE / MERGE / OPTIMIZE, ...) = what Spark did
SELECT version, timestamp, operation
FROM delta.lake."threat_indicators$history"
ORDER BY version;

-- Delta: table properties (protocol versions, etc.)
SELECT * FROM delta.lake."security_events$properties";

-- Column statistics from the Delta log (row count, min/max) without scanning data
SHOW STATS FOR delta.lake.security_events;
```

When working in the interactive shell with `--catalog delta --schema lake`,
you can drop the prefix (e.g. `SHOW TABLES;`, `DESCRIBE security_events;`).

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
