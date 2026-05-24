FROM flink:2.0.1-java17

# ── Versions ──────────────────────────────────────────────────────────────
ARG FLINK_VERSION=2.0.1
ARG ICEBERG_VERSION=1.10.1
ARG HADOOP_VERSION=3.3.6
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0

# Directory Flink auto-loads JARs from
ENV FLINK_LIB=/opt/flink/lib

# ── Kafka SQL connector ────────────────────────────────────────────────────
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    -o ${FLINK_LIB}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# ── Iceberg Flink runtime ─────────────────────────────────────────────────
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/${ICEBERG_VERSION}/iceberg-flink-runtime-2.0-${ICEBERG_VERSION}.jar \
    -o ${FLINK_LIB}/iceberg-flink-runtime-2.0-${ICEBERG_VERSION}.jar

# ── Hadoop client API + runtime ───────────────────────────────────────────
# hadoop-client-api: unshaded public API (Configuration, FileSystem, etc.)
# hadoop-client-runtime: shaded fat jar with all transitive deps (thirdparty)
# Both are required: runtime alone hides the public API classes.
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar \
    -o ${FLINK_LIB}/hadoop-client-api-${HADOOP_VERSION}.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar \
    -o ${FLINK_LIB}/hadoop-client-runtime-${HADOOP_VERSION}.jar

# ── hadoop-aws (S3A connector; iceberg-aws-bundle provides AWS SDK v2) ────
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -o ${FLINK_LIB}/hadoop-aws-${HADOOP_VERSION}.jar

# ── Iceberg AWS bundle (S3FileIO + AWS SDK v2, required by RESTCatalog) ───
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    -o ${FLINK_LIB}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# ── commons-logging (required by iceberg REST client at runtime) ─────────
RUN curl -fsSL \
    https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar \
    -o ${FLINK_LIB}/commons-logging-1.2.jar

# ── Flink S3 FileSystem plugin (Flink 2.x: must live in plugins/, not lib/) ─
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    find /opt/flink/opt -name "flink-s3-fs-hadoop*.jar" \
    -exec cp {} /opt/flink/plugins/s3-fs-hadoop/ \;

# ── Hadoop core-site.xml (S3A / MinIO credentials) ────────────────────────
COPY core-site.xml /opt/flink/conf/core-site.xml
ENV HADOOP_CONF_DIR=/opt/flink/conf