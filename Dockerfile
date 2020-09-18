FROM gradle:6.6-jdk11 AS build
ARG app_version=0.0.0
COPY ./ .
RUN gradle dockerPrepare -Prelease_version=${app_version}

FROM adoptopenjdk/openjdk12:jdk-12.0.2_10-slim
ENV CRADLE_INSTANCE_NAME=instance1 \
    CASSANDRA_DATA_CENTER=kos \
    CASSANDRA_HOST=cassandra \
    CASSANDRA_PORT=9042 \
    CASSANDRA_KEYSPACE=demo \
    CASSANDRA_USERNAME=guest \
    CASSANDRA_PASSWORD=guest \
    HTTP_PORT=8080 \
    HTTP_HOST=localhost
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/servicer", "run", "com.exactpro.th2.reportdataprovider.MainKt"]
