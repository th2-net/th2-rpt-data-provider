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
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class RabbitMqService(
    configuration: Configuration,
    messageRouterParsedBatch: MessageRouter<MessageGroupBatch>,
    private val messageRouterRawBatch: MessageRouter<MessageGroupBatch>
) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()
    private val pendingRequests = ConcurrentHashMap<CodecId, PendingCodecBatchRequest>()
    private val usePinAttributes = configuration.codecUsePinAttributes.value.toBoolean()
    private val maximumPendingRequests = configuration.codecPendingBatchLimit.value.toInt()

    private val mqRequestSenderScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecRequestThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    private val mqCallbackScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecCallbackThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    @Suppress("unused")
    private val receiveChannel = messageRouterParsedBatch.subscribeAll(
        MessageListener { _, decodedBatch ->
            mqCallbackScope.launch {
                val response = MessageGroupBatchWrapper(decodedBatch)

                logger.trace { "codec response with hash ${response.requestHash.hashCode()} has been received" }

                pendingRequests.remove(response.requestHash)?.completableDeferred?.complete(response)
                    ?: logger.debug { "codec response ${response.requestHash} has no matching requests" }
            }
        },

        "from_codec"
    )

    suspend fun sendToCodec(request: CodecBatchRequest): CodecBatchResponse {

        return withContext(mqRequestSenderScope.coroutineContext) {
            while (pendingRequests.keys.size > maximumPendingRequests) {
                delay(100)
            }

            pendingRequests.computeIfAbsent(request.requestHash) {
                val pendingRequest = request.toPending()

                mqCallbackScope.launch {
                    delay(responseTimeout)

                    pendingRequests.remove(request.requestHash)

                    pendingRequest.completableDeferred.let {
                        if (it.isActive) {
                            it.complete(null)
                            logger.warn { "codec request timed out after $responseTimeout ms" }
                        }
                    }
                }

                try {
                    if (usePinAttributes) {
                        val sessionAlias =
                            request.protobufRawMessageBatch.groupsList
                                .first().messagesList
                                .first().rawMessage.metadata.id.connectionId.sessionAlias

                        messageRouterRawBatch.sendAll(request.protobufRawMessageBatch, sessionAlias)
                    } else {
                        messageRouterRawBatch.sendAll(request.protobufRawMessageBatch)
                    }

                    logger.debug { "codec request with hash ${request.requestHash.hashCode()} has been sent" }


                } catch (e: Exception) {
                    pendingRequest.completableDeferred.cancel(
                        "Unexpected exception while trying to send a codec request", e
                    )
                }

                pendingRequest
            }.toResponse()
        }
    }
}

