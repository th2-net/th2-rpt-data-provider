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

import com.exactpro.th2.schema.factory.CommonFactory
import mu.KotlinLogging


class Variable(
    name: String,
    param: String?,
    defaultValue: String,
    showInLog: Boolean = true
) {
    private val logger = KotlinLogging.logger { }

    val value: String = param
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

class CustomConfigurationClass {
    var hostname: String = "localhost"
    var port: Int = 8080
    var responseTimeout: Int = 60000
    var serverCacheTimeout: Int = 60000
    var clientCacheTimeout: Int = 60
    var eventCacheSize: Int = 100000
    var messageCacheSize: Int = 100000
    var ioDispatcherThreadPoolSize: Int = 1
    var cassandraInstance: String = "instance1"
    var cassandraQueryTimeout: Int = 30000

    override fun toString(): String {
        return "CustomConfigurationClass(hostname='$hostname', port=$port, responseTimeout=$responseTimeout, serverCacheTimeout=$serverCacheTimeout, clientCacheTimeout=$clientCacheTimeout, eventCacheSize=$eventCacheSize, messageCacheSize=$messageCacheSize, ioDispatcherThreadPoolSize=$ioDispatcherThreadPoolSize, cassandraInstance='$cassandraInstance', cassandraQueryTimeout=$cassandraQueryTimeout)"
    }
}


class Configuration(args: Array<String>) {

    private val configurationFactory = CommonFactory.createFromArguments(*args)
    private val customConfiguration =
        configurationFactory.getCustomConfiguration(CustomConfigurationClass::class.java)
    private val cradleConfiguration = configurationFactory.cradleConfiguration

    val hostname: Variable = Variable("HTTP_HOST", customConfiguration.hostname, "localhost")
    val port: Variable = Variable("HTTP_PORT", customConfiguration.port.toString(), "8080")
    val responseTimeout: Variable =
        Variable("HTTP_RESPONSE_TIMEOUT", customConfiguration.responseTimeout.toString(), "60000")
    val serverCacheTimeout: Variable =
        Variable("SERVER_CACHE_TIMEOUT", customConfiguration.serverCacheTimeout.toString(), "60000")
    val clientCacheTimeout: Variable =
        Variable("CLIENT_CACHE_TIMEOUT", customConfiguration.clientCacheTimeout.toString(), "60")

    val eventCacheSize: Variable =
        Variable("EVENT_CACHE_SIZE", customConfiguration.eventCacheSize.toString(), "100000")
    val messageCacheSize: Variable =
        Variable("MESSAGE_CACHE_SIZE", customConfiguration.messageCacheSize.toString(), "100000")
    val cassandraDatacenter: Variable = Variable("CASSANDRA_DATA_CENTER", cradleConfiguration.dataCenter, "kos")
    val cassandraHost: Variable = Variable("CASSANDRA_HOST", cradleConfiguration.host, "cassandra")
    val cassandraPort: Variable = Variable("CASSANDRA_PORT", cradleConfiguration.port.toString(), "9042")
    val cassandraKeyspace: Variable = Variable("CASSANDRA_KEYSPACE", cradleConfiguration.keyspace, "demo")

    val cassandraQueryTimeout: Variable =
        Variable("CASSANDRA_QUERY_TIMEOUT", customConfiguration.cassandraQueryTimeout.toString(), "30000")

    val cassandraUsername: Variable = Variable("CASSANDRA_USERNAME", cradleConfiguration.username, "guest")
    val cassandraPassword: Variable = Variable("CASSANDRA_PASSWORD", cradleConfiguration.password, "guest", false)
    val cassandraInstance: Variable =
        Variable("CRADLE_INSTANCE_NAME", customConfiguration.cassandraInstance, "instance1")
    val ioDispatcherThreadPoolSize: Variable =
        Variable("THREAD_POOL_SIZE", customConfiguration.ioDispatcherThreadPoolSize.toString(), "1")
}
