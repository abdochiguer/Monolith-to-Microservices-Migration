FROM quay.io/strimzi/kafka:1.3.0-kafka-4.3.1

USER root:root

RUN mkdir -p /opt/kafka/plugins/debezium-postgres \
    && curl -fL --output /tmp/debezium-postgres.tgz https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.7.0.Final/debezium-connector-postgres-3.7.0.Final-plugin.tar.gz \
    && tar -xzf /tmp/debezium-postgres.tgz -C /opt/kafka/plugins/debezium-postgres \
    && mkdir -p /opt/kafka/plugins/debezium-jdbc \
    && curl -fL --output /tmp/debezium-jdbc.tgz https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-jdbc/3.7.0.Final/debezium-connector-jdbc-3.7.0.Final-plugin.tar.gz \
    && tar -xzf /tmp/debezium-jdbc.tgz -C /opt/kafka/plugins/debezium-jdbc \
    && curl -fL --output /opt/kafka/plugins/debezium-jdbc/postgresql-42.7.7.jar https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar \
    && rm -f /tmp/debezium-postgres.tgz /tmp/debezium-jdbc.tgz

USER 1001
