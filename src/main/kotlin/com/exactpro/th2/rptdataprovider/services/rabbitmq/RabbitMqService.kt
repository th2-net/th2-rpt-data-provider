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

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatch
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.receiveAvailable
import io.ktor.utils.io.errors.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

class RabbitMqService(private val configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger { }

        private val rabbitMqMessageParseGauge: Metrics = Metrics("rabbit_mq_message_parse", "rabbitMqMessageParse")
    }


    private val messageBuffer: Channel<Pair<MessageBatch, List<MessageRequest>>> = Channel(1000)

    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()

    private val decodeRequests = ConcurrentHashMap<MessageID, ConcurrentSkipListSet<MessageRequest>>()

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
                            it.sendMessage(message)
                        }
                    }
                }
            }

            logger.debug { "${decodeRequests.size} decode requests remaining" }
        },
        "from_codec"
    )


    val messageDecoder = CoroutineScope(Dispatchers.Default).launch {
        decodeMessage()
    }


    private suspend fun mergeBatches(messages: Map.Entry<String, MutableList<MessageRequest>>): RawMessageBatch {
        return RawMessageBatch.newBuilder().addAllMessages(
            messages.value.sortedBy { it.rawMessage.metadata.id.sequence }.map { it.rawMessage }
        ).build()
    }


    private fun getRequestDebugInfo(batches: List<Pair<MessageBatch, List<MessageRequest>>>): List<String> {
        return batches.map { (messageBatch, _) ->
            let {
                val session = messageBatch.id.streamName
                val direction = messageBatch.id.direction
                val firstSeqNum = messageBatch.batch.firstOrNull()?.index
                val lastSeqNum = messageBatch.batch.lastOrNull()?.index
                val count = messageBatch.batch.size

                "(session=$session direction=$direction firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
            }
        }
    }


    private suspend fun decodeMessage() {
        coroutineScope {
            while (true) {
                val batches = messageBuffer.receiveAvailable()

                val batchesMap = mutableMapOf<String, MutableList<MessageRequest>>().apply {
                    batches.forEach { getOrPut(it.first.id.streamName, { mutableListOf() }).addAll(it.second) }
                }

                val requestDebugInfo = getRequestDebugInfo(batches)

                val requests = batches.flatMap { it.second }.associateBy { it.id }

                batchesMap.map {
                    launch {
                        val batch = mergeBatches(it)
                        sendMessageBatchToRabbit(batch, requests)
                    }
                }

                try {
                    withTimeout(responseTimeout) {
                        requests.map { async { it.value.get() } }.awaitAll()
                            .also { logger.debug { "codec response received $requestDebugInfo" } }
                    }
                } catch (e: TimeoutCancellationException) {
                    withContext(NonCancellable) {
                        logger.error { "unable to parse messages $requestDebugInfo - timed out after $responseTimeout milliseconds" }
                        requests.map { request ->
                            decodeRequests.remove(request.key)
                            request.value
                        }.forEach {
                            try {
                                it.sendMessage(null)
                            } catch (e: CancellationException) {
                                logger.error { "cancelled channel from message: '${it.rawMessage.metadata.id}'" }
                            }
                        }
                        requests.values.forEach { if (!it.messageIsSend) it.sendMessage(null) }
                    }
                }

                delay(200)
            }
        }
    }

    suspend fun decodeBatch(messageBatch: MessageBatch): Collection<MessageRequest> {
        return withContext(Dispatchers.IO) {
            val rawBatch = messageBatch.batch
                .map { RawMessage.parseFrom(it.content) }
                .map { MessageRequest.build(it, coroutineContext) }

            messageBuffer.send(messageBatch to rawBatch)

            rawBatch.map { async { it.get(); it } }.awaitAll()
        }
    }

    private suspend fun sendMessageBatchToRabbit(batch: RawMessageBatch, requests: Map<MessageID, MessageRequest>) {
        withContext(Dispatchers.IO) {
            logMetrics(rabbitMqMessageParseGauge) {

                var alreadyRequested = true

                requests.forEach {
                    decodeRequests.computeIfAbsent(it.key) {
                        ConcurrentSkipListSet<MessageRequest>().also { alreadyRequested = false }
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
            }
        }
    }
}
