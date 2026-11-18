-- ---------------------------------------------------------------------------
-- Sample Threat Hunting queries (Athena-style analytics over the lake).
--   docker compose exec trino trino -f /opt/sql/02_threat_hunting.sql
-- ---------------------------------------------------------------------------

-- 1. Volume and malicious ratio per day / event type (Silver)
SELECT event_date, event_type,
       count(*)                                       AS events,
       count_if(is_malicious)                         AS malicious,
       round(100.0 * count_if(is_malicious) / count(*), 2) AS pct_malicious
FROM delta.lake.security_events
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- 2. Top 10 most active malicious IPs (Gold)
SELECT indicator, max_threat_score, threat_category, hits, first_seen, last_seen
FROM delta.lake.threat_indicators
WHERE indicator_type = 'ip'
ORDER BY hits DESC
LIMIT 10;

-- 3. Which internal users received mail from phishing domains? (Silver)
SELECT recipient, count(*) AS phishing_emails, count(DISTINCT domain) AS distinct_domains
FROM delta.lake.security_events
WHERE event_type = 'email' AND is_malicious AND threat_category = 'phishing'
GROUP BY recipient
ORDER BY phishing_emails DESC
LIMIT 10;

-- 4. Malicious events that were NOT blocked (detection gaps)
SELECT action, count(*) AS events
FROM delta.lake.security_events
WHERE is_malicious
GROUP BY action
ORDER BY events DESC;

-- 5. Data-quality reconciliation: Bronze (raw) vs Silver (curated)
SELECT (SELECT count(*) FROM hive.bronze.security_events_raw) AS bronze_rows,
       (SELECT count(*) FROM delta.lake.security_events)       AS silver_rows;
