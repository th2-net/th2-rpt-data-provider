package com.exactpro.th2.reportdataprovider

import mu.KotlinLogging


class Variable(
    name: String,
    defaultValue: String,
    showInLog: Boolean = true
) {
    private val logger = KotlinLogging.logger { }

    private val fullName: String = "PROVIDER_$name"

    val value: String = System.getenv(fullName)
        .also {
            logger.info {
                val valueToLog = if (showInLog) it ?: defaultValue else "*****"

                if (it == null)
                    "environment variable '$fullName' is not set - defaulting to '$valueToLog'"
                else
                    "environment variable '$fullName' is set to '$valueToLog'"
            }
        }
        ?: defaultValue
}

class Configuration(
    val hostname: Variable = Variable("HOST", "localhost"),
    val port: Variable = Variable("PORT", "8000"),
    val cassandraDatacenter: Variable = Variable("CASSANDRA_DATACENTER", "kos"),
    val cassandraHost: Variable = Variable("CASSANDRA_HOST", "kos-cassandra01"),
    val cassandraPort: Variable = Variable("CASSANDRA_PORT", "9042"),
    val cassandraKeyspace: Variable = Variable("CASSANDRA_KEYSPACE", "demo"),
    val cassandraUsername: Variable = Variable("CASSANDRA_USERNAME", "testo"),
    val cassandraPassword: Variable = Variable("CASSANDRA_PASSWORD", "testoPWD"),
    val cassandraInstance: Variable = Variable("CASSANDRA_INSTANCE", "instance1")
)
