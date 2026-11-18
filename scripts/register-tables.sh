#!/bin/bash
# Registers every data lake table in Trino. Idempotent: safe to re-run.
#   docker compose exec trino bash /opt/scripts/register-tables.sh
#
# 1) Bronze (Hive/JSON) table + partition discovery (plain SQL, idempotent)
# 2) Silver/Gold Delta tables: delta.system.register_table() fails if the table
#    already exists, so we check information_schema first.
set -euo pipefail

q() { trino --output-format=TSV --execute "$1" 2>/dev/null; }

echo "== Bronze (hive.bronze.security_events_raw)"
trino -f /opt/sql/01_register_tables.sql 2>/dev/null

echo "== Silver / Gold (delta.lake.*)"
q "CREATE SCHEMA IF NOT EXISTS delta.lake" >/dev/null
register() {
  local table="$1" location="$2"
  if [[ "$(q "SELECT count(*) FROM delta.information_schema.tables WHERE table_schema='lake' AND table_name='${table}'")" == "1" ]]; then
    echo "delta.lake.${table}: already registered"
  else
    q "CALL delta.system.register_table(schema_name => 'lake', table_name => '${table}', table_location => '${location}')" >/dev/null
    echo "delta.lake.${table}: registered -> ${location}"
  fi
}
register security_events   s3://datalake/silver/security_events
register threat_indicators s3://datalake/gold/threat_indicators

echo "== Row counts"
q "SELECT 'bronze', count(*) FROM hive.bronze.security_events_raw
   UNION ALL SELECT 'silver', count(*) FROM delta.lake.security_events
   UNION ALL SELECT 'gold',   count(*) FROM delta.lake.threat_indicators"
