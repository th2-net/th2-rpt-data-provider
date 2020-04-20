FROM openjdk:12-alpine

ENV PROVIDER_CASSANDRA_INSTANCE=gradle_instance \
    PROVIDER_CASSANDRA_DATACENTER=data_center \
    PROVIDER_CASSANDRA_HOST=localhost \
    PROVIDER_CASSANDRA_PORT=9142 \
    PROVIDER_CASSANDRA_KEYSPACE=keyspace \
    PROVIDER_CASSANDRA_USERNAME=guest \
    PROVIDER_CASSANDRA_PASSWORD=guest \
    PROVIDER_PORT=9090 \
    PROVIDER_HOST=localhost

WORKDIR /home
COPY ./ .
ENTRYPOINT ["/home/retriever/bin/provider", "run", "com.exactpro.th2.reportdataprovider.MainKt"]
