"""
Delta Lake table maintenance (compaction + cleanup).

Even with Kafka Connect buffering, each ETL micro-batch appends new Parquet
files to Silver. OPTIMIZE bin-packs them into larger files (fixes the
small-file problem on the read side for Trino), and VACUUM removes files that
are no longer referenced by the Delta log.

Usage:
    spark-submit maintenance.py [--retention-hours 168]
"""

import argparse
import os

from pyspark.sql import SparkSession

LAKE = os.getenv("LAKE_BUCKET", "s3a://datalake")
TABLES = [f"{LAKE}/silver/security_events", f"{LAKE}/gold/threat_indicators"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retention-hours", type=int, default=168)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("delta-maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.retention_hours < 168:
        # Delta refuses short retention by default as a safety net; this lab allows it explicitly.
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    for table in TABLES:
        print(f"== OPTIMIZE {table}")
        spark.sql(f"OPTIMIZE delta.`{table}`").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
        print(f"== VACUUM {table} RETAIN {args.retention_hours} HOURS")
        spark.sql(f"VACUUM delta.`{table}` RETAIN {args.retention_hours} HOURS")
    spark.stop()


if __name__ == "__main__":
    main()
