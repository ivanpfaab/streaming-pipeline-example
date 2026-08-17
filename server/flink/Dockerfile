FROM flink:1.20.1-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink/job

COPY server/flink/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

COPY utils ./utils
COPY server/flink/src ./src

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/flink/job
ENV KAFKA_CONNECTOR_JAR=/opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar

CMD ["python", "-u", "src/config.py"]
