-- ---------------------------------------------------------------------------
-- Bronze tables (idempotent). Re-run any time to discover new dt=/hr= partitions.
-- Usually executed through scripts/register-tables.sh (which also registers
-- the Delta tables), but can be run alone:
--   docker compose exec trino trino -f /opt/sql/01_register_tables.sql
-- ---------------------------------------------------------------------------

-- Bronze: raw JSON from Kafka Connect, Hive-style partitions dt=/hr=
CREATE SCHEMA IF NOT EXISTS hive.bronze;

CREATE TABLE IF NOT EXISTS hive.bronze.security_events_raw (
    event_id        varchar,
    event_time      varchar,
    event_type      varchar,
    source_ip       varchar,
    domain          varchar,
    sender          varchar,
    recipient       varchar,
    threat_score    integer,
    threat_category varchar,
    action          varchar,
    kafka_partition integer,
    kafka_offset    bigint,
    dt              varchar,
    hr              varchar
)
WITH (
    external_location = 's3://datalake/bronze/security-events',
    format            = 'JSON',
    partitioned_by    = ARRAY['dt', 'hr']
);

-- New dt=/hr= folders appear every hour: discover them (equivalent to MSCK REPAIR TABLE).
CALL hive.system.sync_partition_metadata('bronze', 'security_events_raw', 'FULL');

