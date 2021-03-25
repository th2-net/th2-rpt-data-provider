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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import io.ktor.utils.io.errors.IOException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

class RabbitMqService(private val configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger { }

        private val rabbitMqMessageParseGauge: Metrics = Metrics.createMetric("rabbit_mq_message_parse", "rabbitMqMessageParse")
    }

    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()

    private val decodeRequests = ConcurrentHashMap<MessageID, ConcurrentSkipListSet<CodecRequest>>()

    private val receiveChannel = configuration.messageRouterParsedBatch.subscribeAll(
        MessageListener { _, decodedBatch ->
            decodedBatch.messagesList.groupBy { it.metadata.id.sequence }.forEach { (_, messages) ->

                val messageId = messages.first().metadata.id.run {
                    when (subsequenceCount) {
                        0 -> this
                        else -> toBuilder().clearSubsequence().build()
                    }
                }

                decodeRequests.remove(messageId)?.let { match ->
                    match.forEach {
                        GlobalScope.launch {
                            val message = when (messages.size) {
                                1 -> messages[0]
                                else -> messages[0].toBuilder().run {
                                    messages.drop(1).forEach { mergeFrom(it) }
                                    metadataBuilder.messageType = messages.joinToString("/") { it.metadata.messageType }
                                    build()
                                }
                            }
                            it.channel.send(message)
                        }
                    }
                }
            }

            logger.debug { "${decodeRequests.size} decode requests remaining" }
        },
        "from_codec"
    )

    @Throws(IllegalStateException::class)
    suspend fun decodeMessage(batch: RawMessageBatch): Collection<Message> {
        val requests: Map<MessageID, CodecRequest> = batch.messagesList
            .associate { it.metadata.id to CodecRequest(it.metadata.id) }

        return withContext(Dispatchers.IO) {
            logMetrics(rabbitMqMessageParseGauge) {
                val deferred = requests.map { async { it.value.channel.receive() } }

                var alreadyRequested = true

                requests.forEach {
                    decodeRequests.computeIfAbsent(it.key) {
                        ConcurrentSkipListSet<CodecRequest>().also { alreadyRequested = false }
                    }.add(it.value)
                }

                val firstId = batch.messagesList?.first()?.metadata?.id
                val requestDebugInfo = let {
                    val session = firstId?.connectionId?.sessionAlias
                    val direction = firstId?.direction?.name
                    val firstSeqNum = firstId?.sequence
                    val lastSeqNum = batch.messagesList?.last()?.metadata?.id?.sequence
                    val count = batch.messagesCount

                    "(session=$session direction=$direction firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
                }

                if (!alreadyRequested) {
                    try {
                        configuration.messageRouterRawBatch.sendAll(batch, firstId?.connectionId?.sessionAlias)
                        logger.debug { "codec request published $requestDebugInfo" }
                    } catch (e: IOException) {
                        logger.error(e) { "cannot send message $requestDebugInfo" }
                    }
                }

                try {
                    withTimeout(responseTimeout) {
                        deferred.awaitAll().also { logger.debug { "codec response received $requestDebugInfo" } }
                    }
                } catch (e: TimeoutCancellationException) {
                    logger.error { "unable to parse messages $requestDebugInfo - timed out after $responseTimeout milliseconds" }
                    requests.map { request ->
                        decodeRequests.remove(request.key)
                        request.value
                    }.forEach {
                        try {
                            it.channel.cancel()
                        } catch (e: CancellationException) {
                            logger.error { "cancelled channel from message: '${it.id}'" }
                        }
                    }
                    deferred.mapNotNull { if (it.isCompleted) it.await() else null }
                }
            } ?: listOf()
        }
    }
}
