/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest.Companion.firstSequence
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest.Companion.lastSequence
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest.Companion.stream
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest.Companion.firstSequence
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest.Companion.lastSequence
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest.Companion.stream
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

abstract class RabbitMqService<B, G, PM>(
    configuration: Configuration
) {

    companion object {
        val K_LOGGER = KotlinLogging.logger { }
        @JvmStatic
        protected val TO_CODEC_ATTRIBUTE_NAME = "to_codec"
        @JvmStatic
        protected val FROM_CODEC_ATTRIBUTE_NAME = "from_codec"
    }

    protected val responseTimeout = configuration.codecResponseTimeout.value.toLong()
    protected val pendingRequests = ConcurrentHashMap<CodecRequestId,  PendingCodecBatchRequest<G, PM>>()

    protected val usePinAttributes = configuration.codecUsePinAttributes.value.toBoolean()

    protected val maximumPendingRequests = configuration.codecPendingBatchLimit.value.toInt()

    protected val mqRequestSenderScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecRequestThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    protected val mqCallbackScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecCallbackThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    protected val mqSubscribeScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    protected val codecLatency = PipelineStatus.codecLatency

    @Suppress("unused")
    protected abstract val receiveChannel: Job

    private data class TimeoutInfo<B, G, PM>(
        val firstSequence: Long?,
        val lastSequence: Long?,
        val stream: String
    ) {
        constructor(request:  CodecBatchRequest<B, G, PM>) : this(
            request.firstSequence,
            request.lastSequence,
            request.stream
        )
    }

    suspend fun sendToCodec(request:  CodecBatchRequest<B, G, PM>):  CodecBatchResponse<G, PM> {

        K_LOGGER.trace { "Send batch before cycle ${request.requestId} with id to codec" }

        while (pendingRequests.keys.size > maximumPendingRequests) {
            delay(100)
        }

        K_LOGGER.trace { "Send batch ${request.requestId} with id to codec" }

        return pendingRequests.computeIfAbsent(request.requestId) {
            val pendingRequest = request.toPending()

            K_LOGGER.trace { "Get pending request for batch ${request.requestId}" }

            val info = if (K_LOGGER.isWarnEnabled) TimeoutInfo(request) else null

            // launch timeout handler coroutine
            val timeoutHandler = mqCallbackScope.launch {

                delay(responseTimeout)

                pendingRequest.let {
                    if (pendingRequests[request.requestId]?.completableDeferred === pendingRequest.completableDeferred) {

                        pendingRequests.remove(request.requestId)
                        it.complete(null)

                        codecLatency.gaugeDec(listOf(request.streamName))
                        codecLatency.setDuration(
                            pendingRequest.startTimestamp.toDouble(),
                            listOf(request.streamName)
                        )

                        K_LOGGER.warn {
                            info?.let { timeout ->
                                "codec request timed out after $responseTimeout ms (stream=${timeout.stream} firstId=${timeout.firstSequence} lastId=${timeout.lastSequence} hash=${request.requestHash}) requestId=${request.requestId}"
                            }
                        }
                    }
                }
            }

            // important! we must save link to timeoutHandler,
            // because it allows us to cancel it when we get parsed message
            pendingRequest.timeoutHandler = timeoutHandler

            K_LOGGER.trace { "Check timeout callback for batch ${request.requestId}" }
            K_LOGGER.trace { "Check mqRequestSenderScope alive ${mqRequestSenderScope.isActive}" }

            mqRequestSenderScope.launch {

                K_LOGGER.trace { "Launch sendAll coroutine ${request.requestId}" }

                measureTimeMillis {
                    try {
                        val sendAllTime = measureTimeMillis {
                            if (usePinAttributes) {
                                val sessionAlias = request.sessionAlias

                                K_LOGGER.trace { "Session aliases for batch ${request.requestId}, alias $sessionAlias" }

                                codecLatency.gaugeInc(listOf(request.streamName))

                                K_LOGGER.trace { "Start sendAll method for batch ${request.requestId}" }

                                measureTimeMillis {
                                    sendAll(request.rawMessageBatch, sessionAlias, TO_CODEC_ATTRIBUTE_NAME)
                                }.also { K_LOGGER.debug { "messageRouterRawBatch ${request.requestId} sendAll ${it}ms" } }
                            } else {
                                measureTimeMillis {
                                    sendAll(request.rawMessageBatch, TO_CODEC_ATTRIBUTE_NAME)
                                }.also { K_LOGGER.debug { "messageRouterRawBatch ${request.requestId} sendAll ${it}ms" } }
                            }

                            K_LOGGER.trace {
                                "codec request with hash ${request.requestHash} has been sent (stream=${request.stream} firstId=${request.firstSequence} lastId=${request.lastSequence} hash=${request.requestHash}) requestId=${request.requestId})"
                            }
                            K_LOGGER.debug { "codec request with hash ${request.requestHash.hashCode()} has been sent" }
                        }

                        StreamWriter.setSendToCodecTime(sendAllTime)
                    } catch (e: Exception) {
                        if (pendingRequests[request.requestId]?.completableDeferred === pendingRequest.completableDeferred) {

                            pendingRequest.complete(null)

                            pendingRequests.remove(request.requestId)
                        }
                        K_LOGGER.error(e) { "unexpected exception while trying to send a codec request" }
                    }
                }.also { K_LOGGER.debug { "${request.requestId} mqRequestSenderScope ${it}ms" } }
            }

            pendingRequest
        }.toResponse()
    }

    protected abstract fun sendAll(batch: B, vararg attributes: String)
}

class ProtoRabbitMqService(
    configuration: Configuration,
    messageRouterParsedBatch: MessageRouter<MessageGroupBatch>,
    private val messageRouter: MessageRouter<MessageGroupBatch>
): RabbitMqService<MessageGroupBatch, ProtoMessageGroup, Message>(configuration) {

    companion object {
        val K_LOGGER = KotlinLogging.logger { }
    }

    @Suppress("unused")
    override val receiveChannel = mqSubscribeScope.launch {
        messageRouterParsedBatch.subscribeAll(
            { _, decodedBatch ->
                mqCallbackScope.launch {

                    val response = ProtoMessageGroupBatchWrapper(decodedBatch)

                    K_LOGGER.trace { "codec response with hash ${response.responseHash} has been received" }

                    pendingRequests.remove(response.requestId)?.let {
                        codecLatency.gaugeDec(listOf(it.streamName))
                        codecLatency.setDuration(it.startTimestamp.toDouble(), listOf(it.streamName))

                        it.complete(response)
                    }
                        ?: K_LOGGER.trace {
                            "codec response with hash ${response.responseHash} has no matching requests (stream=${decodedBatch.stream()} firstId=${decodedBatch.firstSequence()} lastId=${decodedBatch.lastSequence()} requestId=${response.requestId})"
                        }
                }
            },
            FROM_CODEC_ATTRIBUTE_NAME
        )
    }

    override fun sendAll(batch: MessageGroupBatch, vararg attributes: String) {
        messageRouter.sendAll(
            batch,
            *attributes
        )
    }
}

class TransportRabbitMqService(
    configuration: Configuration,
    messageRouterParsedBatch: MessageRouter<GroupBatch>,
    private val messageRouter: MessageRouter<GroupBatch>
): RabbitMqService<GroupBatch, TransportMessageGroup, ParsedMessage>(configuration) {

    companion object {
        val K_LOGGER = KotlinLogging.logger { }
    }

    @Suppress("unused")
    override val receiveChannel = mqSubscribeScope.launch {
        messageRouterParsedBatch.subscribeAll(
            { _, decodedBatch ->
                mqCallbackScope.launch {

                    val response = TransportMessageGroupBatchWrapper(decodedBatch)

                    K_LOGGER.trace { "codec response with hash ${response.responseHash} has been received" }

                    pendingRequests.remove(response.requestId)?.let {
                        codecLatency.gaugeDec(listOf(it.streamName))
                        codecLatency.setDuration(it.startTimestamp.toDouble(), listOf(it.streamName))

                        it.complete(response)
                    }
                        ?: K_LOGGER.trace {
                            "codec response with hash ${response.responseHash} has no matching requests (stream=${decodedBatch.stream()} firstId=${decodedBatch.firstSequence()} lastId=${decodedBatch.lastSequence()} requestId=${response.requestId})"
                        }
                }
            },

            "from_codec"
        )
    }

    override fun sendAll(batch: GroupBatch, vararg attributes: String) {
        messageRouter.sendAll(
            batch,
            *attributes
        )
    }
}
