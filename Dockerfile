FROM gradle:8.11.1-jdk11 AS build
ARG Prelease_version=0.0.0
COPY ./ .
RUN gradle clean build dockerPrepare -Prelease_version=${Prelease_version}

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "run", "com.exactpro.th2.rptdataprovider.MainKt"]
