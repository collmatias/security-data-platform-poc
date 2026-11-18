# Spark 3.5 image that simulates an AWS Glue job runtime.
# All JVM dependencies are baked in at build time so `spark-submit`
# never needs to resolve `--packages` from the internet at runtime.
FROM apache/spark:3.5.3-python3

USER root

# --- S3A (MinIO) support: must match the Hadoop version bundled with Spark (3.3.4)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# --- Delta Lake (3.2.x is the line compatible with Spark 3.5)
ADD --chmod=644 https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar /opt/spark/jars/

# --- Python client for the Redis (ElastiCache) sink
RUN pip3 install --no-cache-dir redis==5.0.8

# Local checkpoint dir (mounted as a named volume; ownership is inherited from the image)
RUN mkdir -p /opt/spark/checkpoints /opt/spark/jobs && chown -R spark:spark /opt/spark/checkpoints /opt/spark/jobs

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark/jobs
