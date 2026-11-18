"""
Bronze -> Silver/Gold ETL (simulates an AWS Glue job).

Input  : raw gzipped JSON files written to MinIO by the Kafka Connect S3 Sink
         (s3a://datalake/bronze/security-events/dt=YYYY-MM-dd/hr=HH/*.json.gz)
Outputs:
  1. Silver  - Delta table with every cleaned/typed event, partitioned by date
               (s3a://datalake/silver/security_events)
  2. Gold    - Delta table with one row per malicious indicator (IP / domain),
               upserted with MERGE (s3a://datalake/gold/threat_indicators)
  3. Redis   - the same malicious indicators as key/value pairs for the
               low-latency detection engine (ElastiCache simulation)

Incremental processing ("Glue job bookmarks") is implemented with the Spark
Structured Streaming *file source*: the checkpoint remembers which bronze files
were already processed, so every run only picks up new files.

Modes:
  --mode batch  : process every new file available right now, then exit
                  (trigger=availableNow; this is the Glue-job style run)
  --mode stream : keep running and poll for new files every --interval seconds
"""

import argparse
import json
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

LAKE = os.getenv("LAKE_BUCKET", "s3a://datalake")
BRONZE_PATH = f"{LAKE}/bronze/security-events"
SILVER_PATH = f"{LAKE}/silver/security_events"
GOLD_PATH = f"{LAKE}/gold/threat_indicators"
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/spark/checkpoints/bronze_to_silver")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", str(7 * 24 * 3600)))
THREAT_THRESHOLD = int(os.getenv("THREAT_THRESHOLD", "80"))

# Used to make Silver appends idempotent across retries of the same micro-batch.
DELTA_TXN_APP_ID = "etl_bronze_to_silver"

log = logging.getLogger("etl")

# Explicit schema: never infer schemas on a streaming source.
BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("source_ip", StringType()),
    StructField("domain", StringType()),
    StructField("sender", StringType()),
    StructField("recipient", StringType()),
    StructField("threat_score", IntegerType()),
    StructField("threat_category", StringType()),
    StructField("action", StringType()),
    # Added by the Kafka Connect InsertField SMT (lineage back to Kafka).
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
])


def build_spark() -> SparkSession:
    # S3A / Delta settings live in /opt/spark/conf/spark-defaults.conf.
    return SparkSession.builder.appName("security-events-etl").getOrCreate()


def transform(raw: DataFrame) -> DataFrame:
    """Bronze -> Silver: type casting, enrichment and basic data quality."""
    return (
        raw
        .withColumn("source_file", F.input_file_name())
        .filter(F.col("event_id").isNotNull() & F.col("threat_score").between(0, 100))
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("domain", F.lower(F.trim("domain")))
        .withColumn("is_malicious", F.col("threat_score") > THREAT_THRESHOLD)
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("event_date", F.date_format("event_ts", "yyyy-MM-dd"))
        .drop("event_time")
    )


def bronze_exists(spark: SparkSession) -> bool:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(path)


def ensure_gold_table(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{GOLD_PATH}` (
            indicator_type   STRING,
            indicator        STRING,
            max_threat_score INT,
            threat_category  STRING,
            hits             BIGINT,
            first_seen       TIMESTAMP,
            last_seen        TIMESTAMP,
            updated_at       TIMESTAMP
        ) USING DELTA
    """)


def indicators_from(events: DataFrame) -> DataFrame:
    """Explode malicious events into (indicator_type, indicator) aggregates."""
    bad = events.filter("is_malicious")
    ips = bad.select(F.lit("ip").alias("indicator_type"), F.col("source_ip").alias("indicator"),
                     "threat_score", "threat_category", "event_ts")
    domains = bad.select(F.lit("domain").alias("indicator_type"), F.col("domain").alias("indicator"),
                         "threat_score", "threat_category", "event_ts")
    return (
        ips.unionByName(domains)
        .filter(F.col("indicator").isNotNull())
        .groupBy("indicator_type", "indicator")
        .agg(
            F.max("threat_score").alias("max_threat_score"),
            F.max_by("threat_category", "threat_score").alias("threat_category"),
            F.count("*").alias("hits"),
            F.min("event_ts").alias("first_seen"),
            F.max("event_ts").alias("last_seen"),
        )
    )


def merge_gold(batch_indicators: DataFrame) -> None:
    # foreachBatch runs on a cloned session: the temp view and the MERGE must use
    # the DataFrame's own session, not the outer `spark` object.
    session = batch_indicators.sparkSession
    batch_indicators.createOrReplaceTempView("batch_indicators")
    session.sql(f"""
        MERGE INTO delta.`{GOLD_PATH}` AS t
        USING batch_indicators AS s
          ON t.indicator_type = s.indicator_type AND t.indicator = s.indicator
        WHEN MATCHED THEN UPDATE SET
          t.max_threat_score = GREATEST(t.max_threat_score, s.max_threat_score),
          t.threat_category  = CASE WHEN s.max_threat_score >= t.max_threat_score
                                    THEN s.threat_category ELSE t.threat_category END,
          t.hits             = t.hits + s.hits,
          t.first_seen       = LEAST(t.first_seen, s.first_seen),
          t.last_seen        = GREATEST(t.last_seen, s.last_seen),
          t.updated_at       = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
          (indicator_type, indicator, max_threat_score, threat_category, hits, first_seen, last_seen, updated_at)
          VALUES (s.indicator_type, s.indicator, s.max_threat_score, s.threat_category, s.hits,
                  s.first_seen, s.last_seen, current_timestamp())
    """)


def write_partition_to_redis(rows) -> None:
    """Runs on executors: one Redis connection + pipeline per partition."""
    import redis  # imported on the executor

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=5)
    pipe = client.pipeline(transaction=False)
    pending = 0
    for r in rows:
        value = json.dumps({
            "score": r.max_threat_score,
            "category": r.threat_category,
            "hits": r.hits,
            "first_seen": r.first_seen.isoformat() if r.first_seen else None,
            "last_seen": r.last_seen.isoformat() if r.last_seen else None,
        })
        # Key layout: threat:<type>:<indicator>  e.g. threat:ip:185.1.2.3
        pipe.set(f"threat:{r.indicator_type}:{r.indicator}", value, ex=REDIS_TTL_SECONDS)
        pending += 1
        if pending >= 1000:
            pipe.execute()
            pending = 0
    if pending:
        pipe.execute()
    client.close()


def process_batch():
    def _process(batch_df: DataFrame, batch_id: int) -> None:
        events = transform(batch_df).dropDuplicates(["event_id"]).persist()
        total = events.count()
        if total == 0:
            events.unpersist()
            return

        # 1) Silver: idempotent append (txnAppId/txnVersion skip replays of the same batch_id).
        (events.write.format("delta")
            .mode("append")
            .option("txnAppId", DELTA_TXN_APP_ID)
            .option("txnVersion", batch_id)
            .partitionBy("event_date")
            .save(SILVER_PATH))

        # 2) Gold: upsert malicious indicators.
        batch_indicators = indicators_from(events).persist()
        n_indicators = batch_indicators.count()
        if n_indicators:
            merge_gold(batch_indicators)

            # 3) Redis: publish the *merged* gold state of the touched indicators,
            #    so the cache always mirrors the gold table.
            keys = batch_indicators.select("indicator_type", "indicator")
            gold_rows = batch_df.sparkSession.read.format("delta").load(GOLD_PATH).join(F.broadcast(keys),
                                                                        ["indicator_type", "indicator"])
            gold_rows.foreachPartition(write_partition_to_redis)

        log.warning("batch=%s events=%s malicious_indicators=%s", batch_id, total, n_indicators)
        batch_indicators.unpersist()
        events.unpersist()

    return _process


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    parser.add_argument("--interval", type=int, default=30, help="Polling seconds in stream mode")
    parser.add_argument("--max-files-per-trigger", type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if not bronze_exists(spark):
        log.warning("No bronze data at %s yet (is the S3 sink connector running?). Nothing to do.", BRONZE_PATH)
        spark.stop()
        return

    ensure_gold_table(spark)

    bronze = (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .option("recursiveFileLookup", "true")      # walk dt=/hr= folders
        .option("pathGlobFilter", "*.json.gz")
        .option("maxFilesPerTrigger", args.max_files_per_trigger)
        .json(BRONZE_PATH)
    )

    writer = (
        bronze.writeStream
        .queryName("bronze_to_silver_gold_redis")
        .foreachBatch(process_batch())
        .option("checkpointLocation", CHECKPOINT)
    )
    writer = writer.trigger(availableNow=True) if args.mode == "batch" \
        else writer.trigger(processingTime=f"{args.interval} seconds")

    query = writer.start()
    query.awaitTermination()
    log.warning("ETL finished (mode=%s)", args.mode)


if __name__ == "__main__":
    main()
