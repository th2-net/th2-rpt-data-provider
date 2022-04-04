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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

class RabbitMqService(
    configuration: Configuration,
    messageRouterParsedBatch: MessageRouter<MessageGroupBatch>,
    private val messageRouterRawBatch: MessageRouter<MessageGroupBatch>
) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val toCodecAttributeName = "to_codec"

    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()
    private val pendingRequests = ConcurrentHashMap<CodecRequestId, PendingCodecBatchRequest>()

    private val usePinAttributes = configuration.codecUsePinAttributes.value.toBoolean()

    private val maximumPendingRequests = configuration.codecPendingBatchLimit.value.toInt()

    private val mqRequestSenderScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecRequestThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    private val mqCallbackScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecCallbackThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    private val mqSubscribeScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    private val codecLatency = PipelineStatus.codecLatency

    @Suppress("unused")
    private val receiveChannel = mqSubscribeScope.launch {
        messageRouterParsedBatch.subscribeAll(
            MessageListener { _, decodedBatch ->
                mqCallbackScope.launch {

                    val response = MessageGroupBatchWrapper(decodedBatch)

                    logger.trace { "codec response with hash ${response.responseHash} has been received" }

                    pendingRequests.remove(response.requestId)?.let {
                        codecLatency.gaugeDec(listOf(it.streamName))
                        codecLatency.setDuration(it.startTimestamp.toDouble(), listOf(it.streamName))
                        it.completableDeferred.complete(response)
                    }
                        ?: logger.trace {
                            val firstSequence =
                                decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.sequence
                            val lastSequence =
                                decodedBatch.groupsList?.lastOrNull()?.messagesList?.lastOrNull()?.sequence
                            val stream =
                                "${decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.message?.sessionAlias}:${decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.message?.direction.toString()}"
                            "codec response with hash ${response.responseHash} has no matching requests (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} requestId=${response.requestId})"
                        }
                }
            },

            "from_codec"
        )
    }

    suspend fun sendToCodec(request: CodecBatchRequest): CodecBatchResponse {

        while (pendingRequests.keys.size > maximumPendingRequests) {
            delay(100)
        }

        return pendingRequests.computeIfAbsent(request.requestId) {
            val pendingRequest = request.toPending()

            mqCallbackScope.launch {
                delay(responseTimeout)

                pendingRequest.completableDeferred.let {
                    if (it.isActive &&
                        pendingRequests[request.requestId]?.completableDeferred == pendingRequest.completableDeferred
                    ) {

                        pendingRequests.remove(request.requestId)
                        it.complete(null)

                        codecLatency.gaugeDec(listOf(request.streamName))
                        codecLatency.setDuration(
                            pendingRequest.startTimestamp.toDouble(),
                            listOf(request.streamName)
                        )

                        logger.warn {
                            val firstSequence =
                                request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sequence
                            val lastSequence =
                                request.protobufRawMessageBatch.groupsList.last()?.messagesList?.last()?.rawMessage?.sequence
                            val stream =
                                "${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sessionAlias}:${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.direction.toString()}"

                            "codec request timed out after $responseTimeout ms (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} hash=${request.requestHash}) requestId=${request.requestId}"
                        }
                    }
                }
            }

            mqRequestSenderScope.launch {
                try {
                    val sendAllTime = measureTimeMillis {
                        if (usePinAttributes) {
                            val sessionAlias =
                                request.protobufRawMessageBatch.groupsList
                                    .first().messagesList
                                    .first().rawMessage.metadata.id.connectionId.sessionAlias

                            codecLatency.gaugeInc(listOf(request.streamName))

                            messageRouterRawBatch.sendAll(
                                request.protobufRawMessageBatch,
                                sessionAlias,
                                toCodecAttributeName
                            )
                        } else {
                            messageRouterRawBatch.sendAll(request.protobufRawMessageBatch, toCodecAttributeName)
                        }

                        logger.trace {
                            val firstSequence =
                                request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sequence
                            val lastSequence =
                                request.protobufRawMessageBatch.groupsList.last()?.messagesList?.last()?.rawMessage?.sequence
                            val stream =
                                "${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sessionAlias}:${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.direction.toString()}"


                            "codec request with hash ${request.requestHash} has been sent (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} hash=${request.requestHash}) requestId=${request.requestId})"
                        }
                        logger.debug { "codec request with hash ${request.requestHash.hashCode()} has been sent" }
                    }
                    StreamWriter.setSendToCodecTime(sendAllTime)
                }
                catch (e: Exception) {
                    pendingRequest.completableDeferred.complete(null)
                    logger.error(e){ "Unexpected exception while trying to send a codec request" }
                }
            }

            pendingRequest
        }.toResponse()
    }
}
