FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

WORKDIR /opt/spark/job

RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY utils ./utils
COPY server/spark/src ./src

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/spark/job:/opt/spark/job/src:/opt/spark/python
ENV PYSPARK_PYTHON=python3

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "src/config.py"]
