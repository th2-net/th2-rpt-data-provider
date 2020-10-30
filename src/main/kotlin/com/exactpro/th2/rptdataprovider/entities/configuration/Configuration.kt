/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.entities.configuration

class Configuration(
    val hostname: Variable = Variable("HTTP_HOST", "localhost"),
    val port: Variable = Variable("HTTP_PORT", "8080"),
    val responseTimeout: Variable = Variable("HTTP_RESPONSE_TIMEOUT", "60000"),
    val codecResponseTimeout: Variable = Variable("CODEC_RESPONSE_TIMEOUT", "6000"),
    val serverCacheTimeout: Variable = Variable("SERVER_CACHE_TIMEOUT", "60000"),

    val eventCacheSize: Variable = Variable("EVENT_CACHE_SIZE", "100"),
    val messageCacheSize: Variable = Variable("MESSAGE_CACHE_SIZE", "100"),
    val codecCacheSize: Variable = Variable("CODEC_CACHE_SIZE", "100"),

    val cassandraDatacenter: Variable = Variable("CASSANDRA_DATA_CENTER", "kos"),
    val cassandraHost: Variable = Variable("CASSANDRA_HOST", "cassandra"),
    val cassandraPort: Variable = Variable("CASSANDRA_PORT", "9042"),
    val cassandraKeyspace: Variable = Variable("CASSANDRA_KEYSPACE", "demo"),
    val cassandraQueryTimeout: Variable = Variable("CASSANDRA_QUERY_TIMEOUT", "30000"),
    val cassandraUsername: Variable = Variable("CASSANDRA_USERNAME", "guest"),
    val cassandraPassword: Variable = Variable("CASSANDRA_PASSWORD", "guest", false),
    val cassandraInstance: Variable = Variable("CRADLE_INSTANCE_NAME", "instance1"),
    val ioDispatcherThreadPoolSize: Variable = Variable("THREAD_POOL_SIZE", "1"),

    val amqpUsername: Variable = Variable("RABBITMQ_USERNAME", ""),
    val amqpPassword: Variable = Variable("RABBITMQ_PASSWORD", "", false),
    val amqpHost: Variable = Variable("RABBITMQ_HOST", ""),
    val amqpPort: Variable = Variable("RABBITMQ_PORT", ""),
    val amqpVhost: Variable = Variable("RABBITMQ_VHOST", ""),

    val amqpCodecExchangeName: Variable = Variable("RABBITMQ_EXCHANGE_NAME_TH2_CODEC", "default_general_exchange"),

    // Class fields are labeled from provider point of view. They are intentionally swapped.
    val amqpCodecRoutingKeyIn: Variable = Variable("RABBITMQ_CODEC_ROUTING_KEY_OUT", "default_general_decode_out"),
    val amqpCodecRoutingKeyOut: Variable = Variable("RABBITMQ_CODEC_ROUTING_KEY_IN", "default_general_decode_in"),

    val amqpProviderQueuePrefix: Variable = Variable("RABBITMQ_PROVIDER_QUEUE_PREFIX", "report-data-provider"),
    val amqpProviderConsumerTag: Variable = Variable("RABBITMQ_PROVIDER_CONSUMER_TAG", "report-data-provider"),


    val enableCaching: Variable = Variable("ENABLE_CACHING", "true"),
    val notModifiedObjectsLifetime: Variable = Variable("NOT_MODIFIED_OBJECTS_LIFETIME", "3600"),
    val rarelyModifiedObjects: Variable = Variable("RARELY_MODIFIED_OBJECTS_LIFETIME", "500"),
    val frequentlyModifiedObjects: Variable = Variable("FREQUENTLY_MODIFIED_OBJECTS_LIFETIME", "100")
)
