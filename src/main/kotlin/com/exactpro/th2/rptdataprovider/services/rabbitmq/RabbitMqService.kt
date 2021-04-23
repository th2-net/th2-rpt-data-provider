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

import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.chunked
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.receiveAvailable
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

    private val rabbitBatchMergeFrequency = configuration.rabbitBatchMergeFrequency.value.toLong()
    private val rabbitBatchMergeBuffer = configuration.rabbitBatchMergeBuffer.value.toInt()
    private val rabbitMergedBatchSize = configuration.rabbitMergedBatchSize.value.toInt()
    private val decodeMessageConsumerCount = configuration.decodeMessageConsumerCount.value.toInt()

    private val messageBuffer = Channel<BatchRequest>(rabbitBatchMergeBuffer * rabbitMergedBatchSize)


    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    private val messageBufferReceiver =
        messageBuffer.chunked(rabbitMergedBatchSize, rabbitBatchMergeFrequency, rabbitBatchMergeBuffer)


    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()

    private val decodeRequests = ConcurrentHashMap<MessageID, ConcurrentSkipListSet<MessageRequest>>()

    private val receiveChannel = configuration.messageRouterParsedBatch.subscribeAll(
        MessageListener { _, decodedBatch ->
            decodedBatch.messagesList.forEach { message ->
                val messageId = message.metadata.id

                decodeRequests.remove(messageId)?.let { match ->

                    match.forEach {
                        GlobalScope.launch { it.sendMessage(message) }
                    }
                }
            }

            logger.debug { "${decodeRequests.size} decode requests remaining" }
        },
        "from_codec"
    )

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    @InternalCoroutinesApi
    private val messageDecoder = GlobalScope.launch {
        for (i in 1..decodeMessageConsumerCount) {
            launch {
                decodeMessage()
            }
        }
    }

    private suspend fun mergeBatches(messages: Map.Entry<String, MutableList<MessageRequest>>): RawMessageBatch {
        return RawMessageBatch.newBuilder().addAllMessages(
            messages.value.sortedBy { it.rawMessage.metadata.id.sequence }.map { it.rawMessage }
        ).build()
    }

    private fun getRequestDebugInfo(batches: MutableMap<String, MutableList<MessageRequest>>): List<String> {
        return batches.map { (session, messageBatch) ->
            let {
                val firstSeqNum = messageBatch.first().id.sequence
                val lastSeqNum = messageBatch.last().id.sequence
                val count = messageBatch.size

                "(session=$session firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
            }
        }
    }


    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    private suspend fun decodeMessage() {
        coroutineScope {
            while (true) {
                val batches = messageBufferReceiver.receive().filter { it.context.isActive }

                if (batches.isEmpty()) continue

                val batchesMap = mutableMapOf<String, MutableList<MessageRequest>>().apply {
                    batches.forEach { getOrPut(it.batch.id.streamName, { mutableListOf() }).addAll(it.requests) }
                }

                val requestDebugInfo = getRequestDebugInfo(batchesMap)

                val requests = batches.flatMap { it.requests }.groupBy { it.id }

                batchesMap.map {
                    launch {
                        val batch = mergeBatches(it)
                        sendMessageBatchToRabbit(batch, requests)
                    }
                }

                try {
                    withTimeout(responseTimeout) {
                        requests.values.flatten().map { async { it.get() } }.awaitAll()
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
                                it.forEach { mes -> mes.sendMessage(null) }
                            } catch (e: CancellationException) {
                                logger.error { "cancelled channel from message: '${it.first().rawMessage.metadata.id}'" }
                            }
                        }
                        requests.values.flatten().forEach { if (!it.messageIsSend) it.sendMessage(null) }
                    }
                }
            }
        }
    }

    suspend fun decodeBatch(messageBatch: StoredMessageBatch): List<MessageRequest> {
        return coroutineScope {
            val rawBatch = messageBatch.messages
                .map { RawMessage.parseFrom(it.content) }
                .map { MessageRequest.build(it) }

            messageBuffer.send(BatchRequest(messageBatch, rawBatch, this))

            rawBatch.map { async { it.get(); it } }.awaitAll()
        }
    }

    private suspend fun sendMessageBatchToRabbit(
        batch: RawMessageBatch,
        requests: Map<MessageID, List<MessageRequest>>
    ) {
        withContext(Dispatchers.IO) {
            logMetrics(rabbitMqMessageParseGauge) {

                var alreadyRequested = true

                requests.forEach {
                    decodeRequests.computeIfAbsent(it.key) {
                        ConcurrentSkipListSet<MessageRequest>().also { alreadyRequested = false }
                    }.addAll(it.value)
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
                    } catch (e: Exception) {
                        logger.error(e) { "cannot send message $requestDebugInfo" }
                    }
                }
            }
        }
    }
}

