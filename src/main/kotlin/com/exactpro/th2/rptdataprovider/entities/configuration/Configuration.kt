/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.rptdataprovider.server.ServerType
import mu.KotlinLogging

class CustomConfigurationClass {
    val hostname: String = "localhost"
    val port: Int = 8080

    val responseTimeout: Int = 60000
    val serverCacheTimeout: Int = 60000

    val eventCacheSize: Int = 1
    val messageCacheSize: Int = 1

    val ioDispatcherThreadPoolSize: Int = 10

    val checkRequestsAliveDelay: Long = 2000

    val enableCaching: Boolean = true
    val notModifiedObjectsLifetime: Int = 3600
    val rarelyModifiedObjects: Int = 500

    val sseEventSearchStep: Long = 200

    val keepAliveTimeout: Long = 5000

    val messageExtractorOutputBatchBuffer: Int = 1
    val messageConverterOutputBatchBuffer: Int = 1
    val messageDecoderOutputBatchBuffer: Int = 1
    val messageUnpackerOutputMessageBuffer: Int = 100
    val messageFilterOutputMessageBuffer: Int = 100
    val messageMergerOutputMessageBuffer: Int = 10

    val codecResponseTimeout: Int = 6000
    val codecPendingBatchLimit: Int = 16
    val codecCallbackThreadPool: Int = 4
    val codecRequestThreadPool: Int = 1
    val codecUsePinAttributes: Boolean = true

    val grpcWriterMessageBuffer: Int = 100

    val grpcThreadPoolSize: Int = 20

    val cradleDispatcherPoolSize: Long = 1

    val sendEmptyDelay = 100

    val eventSearchChunkSize: Int = 64

    val useStrictMode = false

    val serverType: ServerType = ServerType.HTTP
}

class Configuration(customConfiguration: CustomConfigurationClass) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    val hostname: Variable =
        Variable("hostname", customConfiguration.hostname, "localhost")

    val port: Variable =
        Variable("port", customConfiguration.port.toString(), "8080")

    val responseTimeout: Variable =
        Variable("responseTimeout", customConfiguration.responseTimeout.toString(), "60000")

    val serverCacheTimeout: Variable =
        Variable("serverCacheTimeout", customConfiguration.serverCacheTimeout.toString(), "60000")

    val eventCacheSize: Variable =
        Variable("eventCacheSize", customConfiguration.eventCacheSize.toString(), "1")

    val messageCacheSize: Variable =
        Variable("messageCacheSize", customConfiguration.messageCacheSize.toString(), "1")

    val ioDispatcherThreadPoolSize: Variable =
        Variable("ioDispatcherThreadPoolSize", customConfiguration.ioDispatcherThreadPoolSize.let {
            if (it < 10) logger.warn { "The optimal value of the ioDispatcherThreadPoolSize is 10. Current: $it" }

            it.toString()
        }, "10")

    val checkRequestsAliveDelay: Variable =
        Variable("checkRequestsAliveDelay", customConfiguration.checkRequestsAliveDelay.toString(), "2000")

    val enableCaching: Variable = Variable("enableCaching", customConfiguration.enableCaching.toString(), "true")

    val notModifiedObjectsLifetime: Variable =
        Variable("notModifiedObjectsLifetime", customConfiguration.notModifiedObjectsLifetime.toString(), "3600")

    val rarelyModifiedObjects: Variable =
        Variable("rarelyModifiedObjects", customConfiguration.rarelyModifiedObjects.toString(), "500")

    val sseEventSearchStep: Variable =
        Variable("sseEventSearchStep", customConfiguration.sseEventSearchStep.toString(), "200")

    val keepAliveTimeout: Variable =
        Variable("keepAliveTimeout", customConfiguration.keepAliveTimeout.toString(), "5000")

    val messageExtractorOutputBatchBuffer: Variable =
        Variable(
            "messageExtractorOutputBatchBuffer",
            customConfiguration.messageExtractorOutputBatchBuffer.toString(),
            "2"
        )

    val messageConverterOutputBatchBuffer: Variable =
        Variable(
            "messageConverterOutputBatchBuffer",
            customConfiguration.messageConverterOutputBatchBuffer.toString(),
            "2"
        )

    val messageDecoderOutputBatchBuffer: Variable =
        Variable(
            "messageDecoderOutputBatchBuffer",
            customConfiguration.messageDecoderOutputBatchBuffer.toString(),
            "2"
        )

    val messageUnpackerOutputMessageBuffer: Variable =
        Variable(
            "messageUnpackerOutputMessageBuffer",
            customConfiguration.messageUnpackerOutputMessageBuffer.toString(),
            "1000"
        )

    val messageFilterOutputMessageBuffer: Variable =
        Variable(
            "messageFilterOutputMessageBuffer",
            customConfiguration.messageFilterOutputMessageBuffer.toString(),
            "1000"
        )

    val messageMergerOutputMessageBuffer: Variable =
        Variable(
            "messageMergerOutputMessageBuffer",
            customConfiguration.messageMergerOutputMessageBuffer.toString(),
            "10"
        )

    val codecResponseTimeout: Variable = Variable(
        "codecResponseTimeout",
        customConfiguration.codecResponseTimeout.toString(), "6000"
    )

    val codecPendingBatchLimit: Variable =
        Variable("codecPendingBatchLimit", customConfiguration.codecPendingBatchLimit.toString(), "200")

    val codecCallbackThreadPool: Variable =
        Variable("codecCallbackThreadPool", customConfiguration.codecCallbackThreadPool.toString(), "10")

    val codecRequestThreadPool: Variable =
        Variable("codecRequestThreadPool", customConfiguration.codecRequestThreadPool.toString(), "1")

    val grpcWriterMessageBuffer: Variable =
        Variable("grpcWriterMessageBuffer", customConfiguration.grpcWriterMessageBuffer.toString(), "100")

    val cradleDispatcherPoolSize: Variable =
        Variable("cradleDispatcherPoolSize", customConfiguration.cradleDispatcherPoolSize.toString(), "1")

    val sendEmptyDelay: Variable =
        Variable("sendEmptyDelay", customConfiguration.sendEmptyDelay.toString(), "100")

    val eventSearchChunkSize: Variable =
        Variable("eventSearchChunkSize", customConfiguration.eventSearchChunkSize.toString(), "64")

    val useStrictMode: Variable =
        Variable("useStrictMode", customConfiguration.useStrictMode.toString(), "false")

    val serverType: Variable =
        Variable("serverType", customConfiguration.serverType.toString(), "HTTP")

    val codecUsePinAttributes: Variable =
        Variable("codecUsePinAttributes", customConfiguration.codecUsePinAttributes.toString(), "true")

    val grpcThreadPoolSize: Variable =
        Variable("grpcThreadPoolSize", customConfiguration.grpcThreadPoolSize.toString(), "20")

}
