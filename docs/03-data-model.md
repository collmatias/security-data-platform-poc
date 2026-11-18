# 03 — Data Model

## Kafka event (topic `security-events`)

Key: `source_ip` (all events from an IP land in the same partition). Value: JSON.

```json
{
  "event_id": "7f0c1c1e-...",
  "event_time": "2026-11-18T14:16:48.857Z",
  "event_type": "email | web | dns",
  "source_ip": "185.164.238.114",
  "domain": "login-paypa1123.ru",
  "sender": "alice@login-paypa1123.ru",
  "recipient": "bob@corp.local",
  "threat_score": 97,
  "threat_category": "phishing | malware | spam | c2 | credential_theft | bec | suspicious | none",
  "action": "delivered | allowed | quarantined | blocked | rewritten_url"
}
```

The producer sends ~5% malicious events (score 81–100) from a fixed pool of
200 IPs and 150 domains, so indicators repeat the way real campaigns do. Clean
traffic scores 0–80.

## Bronze — `s3://datalake/bronze/security-events/`

```
bronze/security-events/dt=2026-11-18/hr=14/security-events+<partition>+<startOffset>.json.gz
```

- One JSON object per line, gzip. It's the Kafka payload plus `kafka_partition` and
  `kafka_offset` (added by the `InsertField` SMT), for lineage and dedup.
- `dt`/`hr` come from the **Kafka record timestamp** (UTC). They're Hive-style
  partitions, so Trino and Spark can prune them.
- File name = partition + first offset, so a re-delivery after a crash
  overwrites the same object instead of duplicating it.
- Trino table: `hive.bronze.security_events_raw`.

## Silver — `delta.lake.security_events`

| Column | Type | Notes |
|--------|------|-------|
| event_id | string | de-duplicated within each micro-batch |
| event_ts | timestamp | parsed from `event_time` (UTC) |
| event_type, source_ip, domain, sender, recipient | string | `domain` lower-cased/trimmed |
| threat_score | int | rows outside 0–100 are dropped |
| threat_category, action | string | |
| is_malicious | boolean | `threat_score > THREAT_THRESHOLD` (80) |
| kafka_partition, kafka_offset | int, bigint | lineage |
| source_file | string | bronze object that produced the row |
| processed_at | timestamp | ETL time |
| event_date | string | **partition column** (`yyyy-MM-dd`) |

## Gold — `delta.lake.threat_indicators`

One row per `(indicator_type, indicator)`, upserted with `MERGE`.

| Column | Type | Merge rule |
|--------|------|------------|
| indicator_type | string | `ip` or `domain` |
| indicator | string | the IP or domain |
| max_threat_score | int | `GREATEST(old, new)` |
| threat_category | string | category of the highest score seen |
| hits | bigint | `old + new` |
| first_seen / last_seen | timestamp | `LEAST` / `GREATEST` |
| updated_at | timestamp | last MERGE |

## Redis — threat intel cache

| Key | Value | TTL |
|-----|-------|-----|
| `threat:ip:<ip>` | JSON `{score, category, hits, first_seen, last_seen}` | 7 days (`REDIS_TTL_SECONDS`) |
| `threat:domain:<domain>` | same | 7 days |

Design notes:

- Plain `GET` = O(1), one round trip. That's what an AV/MTA plugin needs on the hot path.
  A miss (`nil`) means "not a known threat".
- The value is written **from the gold table after the MERGE**, so Redis always
  mirrors gold (accumulated hits, max score), not just the latest batch.
- The TTL ages out indicators that stop being seen. Each new sighting refreshes it.
- `maxmemory-policy volatile-lru` evicts TTL'd keys first if memory runs out.
