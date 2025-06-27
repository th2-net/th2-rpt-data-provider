/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.rptdataprovider.entities.configuration

import com.exactpro.th2.rptdataprovider.server.ServerType
import io.github.oshai.kotlinlogging.KotlinLogging

class CustomConfigurationClass {
    val hostname: String = "localhost"
    val port: Int = 8080

    val responseTimeout: Int = 60_000
    val serverCacheTimeout: Int = 60_000

    val eventCacheSize: Int = 1
    val messageCacheSize: Int = 1
    val aliasToGroupCacheSize: Int = 1_000

    val ioDispatcherThreadPoolSize: Int = 10

    val checkRequestsAliveDelay: Long = 2_000

    val enableCaching: Boolean = true
    val notModifiedObjectsLifetime: Int = 3_600
    val rarelyModifiedObjects: Int = 500

    val sseEventSearchStep: Long = 200

    val keepAliveTimeout: Long = 5_000
    val ignoreLimitForParent: Boolean = false

    val messageExtractorOutputBatchBuffer: Int = 1
    val messageConverterOutputBatchBuffer: Int = 1
    val messageDecoderOutputBatchBuffer: Int = 1
    val messageUnpackerOutputMessageBuffer: Int = 100
    val messageFilterOutputMessageBuffer: Int = 100
    val messageMergerOutputMessageBuffer: Int = 10
    val messageIdsLookupLimitDays: Int = 7

    val codecResponseTimeout: Int = 6_000
    val codecPendingBatchLimit: Int = 16
    val codecCallbackThreadPool: Int = 4
    val codecRequestThreadPool: Int = 1
    val codecUsePinAttributes: Boolean = true

    val grpcWriterMessageBuffer: Int = 100

    val grpcThreadPoolSize: Int = 20

    val eventSearchTimeOffset = 5_000

    val cradleDispatcherPoolSize: Long = 1

    val sendEmptyDelay = 100

    val eventSearchChunkSize: Int = 64

    val eventSearchGap: Int = 60

    val useStrictMode = false

    val useTransportMode: Boolean = true

    val serverType: ServerType = ServerType.HTTP
}

class Configuration(customConfiguration: CustomConfigurationClass) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    val hostname: Variable =
        Variable("hostname", customConfiguration.hostname)

    val port: Variable =
        Variable("port", customConfiguration.port.toString())

    val responseTimeout: Variable =
        Variable("responseTimeout", customConfiguration.responseTimeout.toString())

    val serverCacheTimeout: Variable =
        Variable("serverCacheTimeout", customConfiguration.serverCacheTimeout.toString())

    val eventCacheSize: Variable =
        Variable("eventCacheSize", customConfiguration.eventCacheSize.toString())

    val messageCacheSize: Variable =
        Variable("messageCacheSize", customConfiguration.messageCacheSize.toString())

    val aliasToGroupCacheSize: Variable = // TODO: added value check
        Variable("aliasToGroupCacheSize", customConfiguration.aliasToGroupCacheSize.toString())

    val useTransportMode: Variable =
        Variable("useTransportMode", customConfiguration.useTransportMode.toString())

    val ioDispatcherThreadPoolSize: Variable =
        Variable("ioDispatcherThreadPoolSize", customConfiguration.ioDispatcherThreadPoolSize.let {
            if (it < 10) logger.warn { "The optimal value of the ioDispatcherThreadPoolSize is 10. Current: $it" }

            it.toString()
        })

    val checkRequestsAliveDelay: Variable =
        Variable("checkRequestsAliveDelay", customConfiguration.checkRequestsAliveDelay.toString())

    val enableCaching: Variable = Variable("enableCaching", customConfiguration.enableCaching.toString())

    val notModifiedObjectsLifetime: Variable =
        Variable("notModifiedObjectsLifetime", customConfiguration.notModifiedObjectsLifetime.toString())

    val rarelyModifiedObjects: Variable =
        Variable("rarelyModifiedObjects", customConfiguration.rarelyModifiedObjects.toString())

    val sseEventSearchStep: Variable =
        Variable("sseEventSearchStep", customConfiguration.sseEventSearchStep.toString())

    val keepAliveTimeout: Variable =
        Variable("keepAliveTimeout", customConfiguration.keepAliveTimeout.toString())

    val ignoreLimitForParent: Variable =
        Variable("ignoreLimitForParent", customConfiguration.ignoreLimitForParent.toString())

    val messageExtractorOutputBatchBuffer: Variable =
        Variable(
            "messageExtractorOutputBatchBuffer",
            customConfiguration.messageExtractorOutputBatchBuffer.toString()
        )

    val messageConverterOutputBatchBuffer: Variable =
        Variable(
            "messageConverterOutputBatchBuffer",
            customConfiguration.messageConverterOutputBatchBuffer.toString()
        )

    val messageDecoderOutputBatchBuffer: Variable =
        Variable(
            "messageDecoderOutputBatchBuffer",
            customConfiguration.messageDecoderOutputBatchBuffer.toString()
        )

    val messageUnpackerOutputMessageBuffer: Variable =
        Variable(
            "messageUnpackerOutputMessageBuffer",
            customConfiguration.messageUnpackerOutputMessageBuffer.toString()
        )

    val messageFilterOutputMessageBuffer: Variable =
        Variable(
            "messageFilterOutputMessageBuffer",
            customConfiguration.messageFilterOutputMessageBuffer.toString()
        )

    val messageMergerOutputMessageBuffer: Variable =
        Variable(
            "messageMergerOutputMessageBuffer",
            customConfiguration.messageMergerOutputMessageBuffer.toString()
        )

    val messageIdsLookupLimitDays: Variable = Variable(
        "messageIdsLookupLimitDays",
        customConfiguration.messageIdsLookupLimitDays.toString()
    )

    val codecResponseTimeout: Variable = Variable(
        "codecResponseTimeout",
        customConfiguration.codecResponseTimeout.toString()
    )

    val codecPendingBatchLimit: Variable =
        Variable("codecPendingBatchLimit", customConfiguration.codecPendingBatchLimit.toString())

    val codecCallbackThreadPool: Variable =
        Variable("codecCallbackThreadPool", customConfiguration.codecCallbackThreadPool.toString())

    val codecRequestThreadPool: Variable =
        Variable("codecRequestThreadPool", customConfiguration.codecRequestThreadPool.toString())

    val grpcWriterMessageBuffer: Variable =
        Variable("grpcWriterMessageBuffer", customConfiguration.grpcWriterMessageBuffer.toString())

    val cradleDispatcherPoolSize: Variable =
        Variable("cradleDispatcherPoolSize", customConfiguration.cradleDispatcherPoolSize.toString())

    val sendEmptyDelay: Variable =
        Variable("sendEmptyDelay", customConfiguration.sendEmptyDelay.toString())

    val eventSearchChunkSize: Variable =
        Variable("eventSearchChunkSize", customConfiguration.eventSearchChunkSize.toString())

    val useStrictMode: Variable =
        Variable("useStrictMode", customConfiguration.useStrictMode.toString())

    val serverType: Variable =
        Variable("serverType", customConfiguration.serverType.toString())

    val codecUsePinAttributes: Variable =
        Variable("codecUsePinAttributes", customConfiguration.codecUsePinAttributes.toString())

    val grpcThreadPoolSize: Variable =
        Variable("grpcThreadPoolSize", customConfiguration.grpcThreadPoolSize.toString())

    val eventSearchTimeOffset: Variable =
        Variable("eventSearchTimeOffset", customConfiguration.eventSearchTimeOffset.toString())

    val eventSearchGap: Variable =
        Variable("eventSearchGap", customConfiguration.eventSearchGap.toString())
}
