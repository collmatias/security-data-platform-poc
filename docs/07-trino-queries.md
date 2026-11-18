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
