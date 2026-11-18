# Kafka Connect worker with the Confluent S3 Sink connector pre-installed.
# The plugin is downloaded in a throw-away stage so the final image has no
# build tools and the connector never needs to be fetched at runtime.
FROM alpine:3.20 AS plugin
ARG S3_SINK_VERSION=10.5.17
RUN apk add --no-cache curl unzip \
 && curl -fsSL -o /tmp/s3-sink.zip \
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/${S3_SINK_VERSION}/confluentinc-kafka-connect-s3-${S3_SINK_VERSION}.zip" \
 && mkdir -p /plugins \
 && unzip -q /tmp/s3-sink.zip -d /plugins

FROM confluentinc/cp-kafka-connect:7.7.1
COPY --from=plugin /plugins /usr/share/confluent-hub-components
