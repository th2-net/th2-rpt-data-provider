/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider

import mu.KotlinLogging


class Variable(
    name: String,
    defaultValue: String,
    showInLog: Boolean = true
) {
    private val logger = KotlinLogging.logger { }

    val value: String = System.getenv(name)
        .also {
            logger.info {
                val valueToLog = if (showInLog) it ?: defaultValue else "*****"

                if (it == null)
                    "environment variable '$name' is not set - defaulting to '$valueToLog'"
                else
                    "environment variable '$name' is set to '$valueToLog'"
            }
        }
        ?: defaultValue
}

class Configuration(
    val hostname: Variable = Variable("HTTP_HOST", "localhost"),
    val port: Variable = Variable("HTTP_PORT", "8080"),
    val responseTimeout: Variable = Variable("HTTP_RESPONSE_TIMEOUT", "1002000"),
    val serverCacheTimeout: Variable = Variable("SERVER_CACHE_TIMEOUT", "1002000"),
    val clientCacheTimeout: Variable = Variable("CLIENT_CACHE_TIMEOUT", "60"),

    val eventCacheSize: Variable = Variable("EVENT_CACHE_SIZE", "100000"),
    val messageCacheSize: Variable = Variable("MESSAGE_CACHE_SIZE", "100000"),
    val cassandraDatacenter: Variable = Variable("CASSANDRA_DATA_CENTER", "kos"),
    val cassandraHost: Variable = Variable("CASSANDRA_HOST", "cassandra"),
    val cassandraPort: Variable = Variable("CASSANDRA_PORT", "9042"),
    val cassandraKeyspace: Variable = Variable("CASSANDRA_KEYSPACE", "demo"),
    val cassandraQueryTimeout: Variable = Variable("CASSANDRA_QUERY_TIMEOUT", "30000"),
    val cassandraUsername: Variable = Variable("CASSANDRA_USERNAME", "guest"),
    val cassandraPassword: Variable = Variable("CASSANDRA_PASSWORD", "guest", false),
    val cassandraInstance: Variable = Variable("CRADLE_INSTANCE_NAME", "instance1"),
    val ioDispatcherThreadPoolSize: Variable = Variable("THREAD_POOL_SIZE", "1")
)
