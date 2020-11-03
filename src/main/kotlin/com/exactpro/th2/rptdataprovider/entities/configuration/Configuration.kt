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

import com.exactpro.cradle.CradleManager
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter

class CustomConfigurationClass {
    var hostname: String = "localhost"
    var port: Int = 8080
    var responseTimeout: Int = 60000
    var serverCacheTimeout: Int = 60000
    var clientCacheTimeout: Int = 60
    var eventCacheSize: Int = 100000
    var messageCacheSize: Int = 100000
    var ioDispatcherThreadPoolSize: Int = 1
    var codecResponseTimeout: Int = 6000
    var codecCacheSize: Int = 100
    var amqpUsername: String = ""
    var amqpPassword: String = ""
    var amqpHost: String = ""
    var amqpPort: String = ""
    var amqpVhost: String = ""

    var amqpCodecExchangeName: String = "default_general_exchange"

    // Class fields are labeled from provider point of view. They are intentionally swapped.
    var amqpCodecRoutingKeyIn: String = "default_general_decode_out"
    var amqpCodecRoutingKeyOut: String = "default_general_decode_in"

    var amqpProviderQueuePrefix: String = "report-data-provider"
    var amqpProviderConsumerTag: String = "report-data-provider"

    val enableCaching: Boolean = true
    val notModifiedObjectsLifetime: Int = 3600
    val rarelyModifiedObjects: Int = 500
    val frequentlyModifiedObjects: Int = 100

    override fun toString(): String {
        return "CustomConfigurationClass(hostname='$hostname', port=$port, responseTimeout=$responseTimeout, serverCacheTimeout=$serverCacheTimeout, clientCacheTimeout=$clientCacheTimeout, eventCacheSize=$eventCacheSize, messageCacheSize=$messageCacheSize, ioDispatcherThreadPoolSize=$ioDispatcherThreadPoolSize, codecResponseTimeout=$codecResponseTimeout, codecCacheSize=$codecCacheSize, amqpUsername='$amqpUsername', amqpPassword='$amqpPassword', amqpHost='$amqpHost', amqpPort='$amqpPort', amqpVhost='$amqpVhost', amqpCodecExchangeName='$amqpCodecExchangeName', amqpCodecRoutingKeyIn='$amqpCodecRoutingKeyIn', amqpCodecRoutingKeyOut='$amqpCodecRoutingKeyOut', amqpProviderQueuePrefix='$amqpProviderQueuePrefix', amqpProviderConsumerTag='$amqpProviderConsumerTag', enableCaching=$enableCaching, notModifiedObjectsLifetime=$notModifiedObjectsLifetime, rarelyModifiedObjects=$rarelyModifiedObjects, frequentlyModifiedObjects=$frequentlyModifiedObjects)"
    }
}

class Configuration(args: Array<String>) {

    private val configurationFactory = CommonFactory.createFromArguments(*args)

    val cradleManager: CradleManager
        get() = configurationFactory.cradleManager

    val messageRouterRawBatch: MessageRouter<RawMessageBatch>
        get() = configurationFactory.messageRouterRawBatch

    val messageRouterParsedBatch: MessageRouter<MessageBatch>
        get() = configurationFactory.messageRouterParsedBatch


    private val customConfiguration =
        configurationFactory.getCustomConfiguration(CustomConfigurationClass::class.java)

    val hostname: Variable =
        Variable("hostname", customConfiguration.hostname, "localhost")

    val port: Variable =
        Variable("port", customConfiguration.port.toString(), "8080")

    val responseTimeout: Variable =
        Variable("responseTimeout", customConfiguration.responseTimeout.toString(), "60000")

    val codecResponseTimeout: Variable = Variable(
        "codecResponseTimeout",
        customConfiguration.codecResponseTimeout.toString(), "6000"
    )

    val serverCacheTimeout: Variable =
        Variable("serverCacheTimeout", customConfiguration.serverCacheTimeout.toString(), "60000")

    val eventCacheSize: Variable =
        Variable("eventCacheSize", customConfiguration.eventCacheSize.toString(), "100000")

    val messageCacheSize: Variable =
        Variable("messageCacheSize", customConfiguration.messageCacheSize.toString(), "100000")

    val codecCacheSize: Variable = Variable(
        "codecCacheSize", customConfiguration.codecCacheSize.toString(),
        "100"
    )

    val ioDispatcherThreadPoolSize: Variable =
        Variable("ioDispatcherThreadPoolSize", customConfiguration.ioDispatcherThreadPoolSize.toString(), "1")

    val amqpUsername: Variable = Variable("amqpUsername", customConfiguration.amqpUsername, "")
    val amqpPassword: Variable = Variable("amqpPassword", customConfiguration.amqpPassword, "", false)
    val amqpHost: Variable = Variable("amqpHost", customConfiguration.amqpHost, "")
    val amqpPort: Variable = Variable("amqpPort", customConfiguration.amqpPort, "")
    val amqpVhost: Variable = Variable("amqpVhost", customConfiguration.amqpVhost, "")

    val amqpCodecExchangeName: Variable =
        Variable("amqpCodecExchangeName", customConfiguration.amqpCodecExchangeName, "default_general_exchange")

    // Class fields are labeled from provider point of view. They are intentionally swapped.
    val amqpCodecRoutingKeyIn: Variable =
        Variable("amqpCodecRoutingKeyIn", customConfiguration.amqpCodecRoutingKeyIn, "default_general_decode_out")
    val amqpCodecRoutingKeyOut: Variable =
        Variable("amqpCodecRoutingKeyOut", customConfiguration.amqpCodecRoutingKeyOut, "default_general_decode_in")

    val amqpProviderQueuePrefix: Variable =
        Variable("amqpProviderQueuePrefix", customConfiguration.amqpProviderQueuePrefix, "report-data-provider")
    val amqpProviderConsumerTag: Variable =
        Variable("amqpProviderConsumerTag", customConfiguration.amqpProviderConsumerTag, "report-data-provider")

    val enableCaching: Variable = Variable("enableCaching", customConfiguration.enableCaching.toString(), "true")
    val notModifiedObjectsLifetime: Variable =
        Variable("notModifiedObjectsLifetime", customConfiguration.notModifiedObjectsLifetime.toString(), "3600")
    val rarelyModifiedObjects: Variable =
        Variable("rarelyModifiedObjects", customConfiguration.rarelyModifiedObjects.toString(), "500")
    val frequentlyModifiedObjects: Variable =
        Variable("frequentlyModifiedObjects", customConfiguration.frequentlyModifiedObjects.toString(), "100")
}