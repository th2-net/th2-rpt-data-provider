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
import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging

class CustomConfigurationClass {
    var hostname: String = "localhost"
    var port: Int = 8080
    var responseTimeout: Int = 60000
    var serverCacheTimeout: Int = 60000
    var clientCacheTimeout: Int = 60
    var eventCacheSize: Int = 100000
    var messageCacheSize: Int = 100000
    var ioDispatcherThreadPoolSize: Int = 10
    var codecResponseTimeout: Int = 6000
    var codecCacheSize: Int = 100
    var codecBatchesCacheSize: Int = 100

    var checkRequestsAliveDelay: Long = 2000
    val enableCaching: Boolean = true
    val notModifiedObjectsLifetime: Int = 3600
    val rarelyModifiedObjects: Int = 500
    val maxMessagesLimit: Int = 500
    val messageSearchPipelineBuffer: Int = 25
    val sseEventSearchStep: Long = 200
    val keepAliveTimeout: Long = 5000
    val dbRetryDelay: Long = 5000

    val cradleDispatcherPoolSize: Long = 1

    val sseSearchDelay: Long = 5
    val rabbitBatchMergeFrequency: Long = 200
    val rabbitBatchMergeBuffer: Long = 500
    val rabbitMergedBatchSize: Long = 16
    val decodeMessageConsumerCount: Int = 64

    val eventSearchChunkSize: Int = 64

    val serverType: String = "http"

    override fun toString(): String {
        return "CustomConfigurationClass(hostname='$hostname', port=$port, responseTimeout=$responseTimeout, serverCacheTimeout=$serverCacheTimeout, clientCacheTimeout=$clientCacheTimeout, eventCacheSize=$eventCacheSize, messageCacheSize=$messageCacheSize, ioDispatcherThreadPoolSize=$ioDispatcherThreadPoolSize, codecResponseTimeout=$codecResponseTimeout, codecCacheSize=$codecCacheSize, codecBatchesCacheSize=$codecBatchesCacheSize, checkRequestsAliveDelay=$checkRequestsAliveDelay, enableCaching=$enableCaching, notModifiedObjectsLifetime=$notModifiedObjectsLifetime, rarelyModifiedObjects=$rarelyModifiedObjects, maxMessagesLimit=$maxMessagesLimit, messageSearchPipelineBuffer=$messageSearchPipelineBuffer, sseEventSearchStep=$sseEventSearchStep, keepAliveTimeout=$keepAliveTimeout, dbRetryDelay=$dbRetryDelay, cradleDispatcherPoolSize=$cradleDispatcherPoolSize, sseSearchDelay=$sseSearchDelay, rabbitBatchMergeFrequency=$rabbitBatchMergeFrequency, rabbitBatchMergeBuffer=$rabbitBatchMergeBuffer, rabbitMergedBatchSize=$rabbitMergedBatchSize, decodeMessageConsumerCount=$decodeMessageConsumerCount, eventSearchChunkSize=$eventSearchChunkSize, serverType='$serverType')"
    }
}

class Configuration(args: Array<String>) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val configurationFactory = CommonFactory.createFromArguments(*args)

    val cradleManager: CradleManager
        get() = configurationFactory.cradleManager

    val messageRouterRawBatch: MessageRouter<RawMessageBatch>
        get() = configurationFactory.messageRouterRawBatch

    val messageRouterParsedBatch: MessageRouter<MessageBatch>
        get() = configurationFactory.messageRouterParsedBatch

    val grpcConfig: GrpcEndpointConfiguration
        get() = configurationFactory.grpcRouterConfiguration.serverConfiguration

    val grpcRouter: GrpcRouter
        get() = configurationFactory.grpcRouter

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

    val codecBatchesCacheSize: Variable = Variable(
        "codecBatchesCacheSize", customConfiguration.codecBatchesCacheSize.toString(),
        "100"
    )

    val ioDispatcherThreadPoolSize: Variable =
        Variable("ioDispatcherThreadPoolSize", customConfiguration.ioDispatcherThreadPoolSize.let {
            if (it < 10) logger.warn { "The optimal value of the ioDispatcherThreadPoolSize is 10. Current: $it" }

            it.toString()
        }, "10")

    val enableCaching: Variable = Variable("enableCaching", customConfiguration.enableCaching.toString(), "true")
    val notModifiedObjectsLifetime: Variable =
        Variable("notModifiedObjectsLifetime", customConfiguration.notModifiedObjectsLifetime.toString(), "3600")
    val rarelyModifiedObjects: Variable =
        Variable("rarelyModifiedObjects", customConfiguration.rarelyModifiedObjects.toString(), "500")

    val checkRequestsAliveDelay: Variable =
        Variable("checkRequestsAliveDelay", customConfiguration.checkRequestsAliveDelay.toString(), "2000")

    val maxMessagesLimit: Variable =
        Variable("maxMessagesLimit", customConfiguration.maxMessagesLimit.toString(), "150")

    val messageSearchPipelineBuffer: Variable =
        Variable("messageSearchPipelineBuffer", customConfiguration.messageSearchPipelineBuffer.toString(), "25")

    val sseEventSearchStep: Variable =
        Variable("sseEventSearchStep", customConfiguration.sseEventSearchStep.toString(), "200")

    val keepAliveTimeout: Variable =
        Variable("keepAliveTimeout", customConfiguration.keepAliveTimeout.toString(), "5000")

    val dbRetryDelay: Variable = Variable("dbRetryDelay", customConfiguration.dbRetryDelay.toString(), "5000")

    val cradleDispatcherPoolSize: Variable =
        Variable("cradleDispatcherPoolSize", customConfiguration.cradleDispatcherPoolSize.toString(), "2")


    val sseSearchDelay: Variable = Variable("sseSearchDelay", customConfiguration.sseSearchDelay.toString(), "5")

    val rabbitBatchMergeFrequency: Variable =
        Variable("rabbitBatchMergeFrequency", customConfiguration.rabbitBatchMergeFrequency.toString(), "200")

    val rabbitBatchMergeBuffer: Variable =
        Variable("rabbitBatchMergeBuffer", customConfiguration.rabbitBatchMergeBuffer.toString(), "20")

    val eventSearchChunkSize: Variable =
        Variable("eventSearchChunkSize", customConfiguration.eventSearchChunkSize.toString(), "64")

    val rabbitMergedBatchSize: Variable =
        Variable("rabbitMergedBatchSize", customConfiguration.rabbitMergedBatchSize.toString(), "64")

    val decodeMessageConsumerCount: Variable =
        Variable("decodeMessageConsumerCount", customConfiguration.decodeMessageConsumerCount.toString(), "64")

    val serverType: Variable =
        Variable("serverType", customConfiguration.serverType, "http")

}