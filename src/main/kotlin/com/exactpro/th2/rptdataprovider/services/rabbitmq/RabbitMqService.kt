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

import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.logMetrics
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.Executors

class RabbitMqService(
    private val configuration: Configuration,
    private val messageRouterParsedBatch: MessageRouter<MessageBatch>,
    private val messageRouterRawBatch: MessageRouter<RawMessageBatch>
) {

    companion object {
        val logger = KotlinLogging.logger { }

        private val rabbitMqBatchParseMetrics: Metrics = Metrics("rabbit_mq_batch_parse", "rabbitMqBatchParse")

    }

    private val mqDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()
    private val mqDispatcher = Executors.newFixedThreadPool(mqDispatcherPoolSize).asCoroutineDispatcher()

    //    private val rabbitBatchMergeFrequency = configuration.rabbitBatchMergeFrequency.value.toLong()
    private val rabbitBatchMergeBuffer = configuration.rabbitBatchMergeBuffer.value.toInt()
    private val rabbitMergedBatchSize = configuration.rabbitMergedBatchSize.value.toInt()
    private val decodeMessageConsumerCount = configuration.decodeMessageConsumerCount.value.toInt()

    private val messageBatchBuffer = Channel<List<BatchRequest>>(rabbitBatchMergeBuffer * rabbitMergedBatchSize)

//    private val messageBuffer = Channel<BatchRequest>(rabbitBatchMergeBuffer * rabbitMergedBatchSize)
//
//    @ExperimentalCoroutinesApi
//    @ObsoleteCoroutinesApi
//    @InternalCoroutinesApi
//    private val messageBufferReceiver =
//        messageBuffer.chunked(rabbitMergedBatchSize, rabbitBatchMergeFrequency, rabbitBatchMergeBuffer)


    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()

    private val decodeRequests = ConcurrentHashMap<MessageID, ConcurrentSkipListSet<MessageRequest>>()

    private val receiveChannel = messageRouterParsedBatch.subscribeAll(
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
                        CoroutineScope(mqDispatcher).launch {
                            it.sendMessage(messages.map { BodyWrapper(it) })
                        }
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
        repeat(decodeMessageConsumerCount) {
            launch {
                decodeMessage()
            }
        }
    }

    private fun mergeBatches(messages: Map.Entry<String, MutableList<MessageRequest>>): RawMessageBatch {
        return RawMessageBatch.newBuilder().addAllMessages(
            messages.value.map { it.rawMessage }
        ).build()
    }

    private fun mergeBatches(messages: List<MessageRequest>): RawMessageBatch {
        return RawMessageBatch.newBuilder().addAllMessages(
            messages.map { it.rawMessage }
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


    private fun getRequestDebugInfo(batches: List<MessageRequest>): String {

        val session = batches.first().id.connectionId.sessionAlias
        val firstSeqNum = batches.first().id.sequence
        val lastSeqNum = batches.last().id.sequence
        val count = batches.size

        return "(session=$session firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
    }


    //    @ExperimentalCoroutinesApi
//    @ObsoleteCoroutinesApi
//    @InternalCoroutinesApi
//    private suspend fun decodeMessage() {
//        coroutineScope {
//            while (true) {
//                val batches = messageBufferReceiver.receive().filter { it.context.isActive }
//
//                if (batches.isEmpty()) continue
//
//                val batchesMap = mutableMapOf<String, MutableList<MessageRequest>>().apply {
//                    batches.forEach {
//                        getOrPut(it.batchId.streamName, { mutableListOf() })
//                            .addAll(it.requests.filterNotNull())
//                    }
//                }
//
//                val requestDebugInfo = getRequestDebugInfo(batchesMap)
//
//                val requestsMaps =
//                    batchesMap.map { group -> group.key to group.value.groupBy { it.id } }.associate { it }
//
//                val requests = batches.flatMap { it.requests }
//
//                try {
//                    logMetrics(rabbitMqBatchParseMetrics) {
//                        batchesMap.map { messages ->
//                            val batch = mergeBatches(messages)
//                            sendMessageBatchToRabbit(batch, requestsMaps.getValue(messages.key))
//                        }
//
//                        withTimeout(responseTimeout) {
//                            requests.map { async { it?.get() } }.awaitAll()
//                                .also { logger.debug { "codec response received $requestDebugInfo" } }
//                        }
//                    }
//                } catch (e: Exception) {
//                    withContext(NonCancellable) {
//                        when (e) {
//                            is TimeoutCancellationException ->
//                                logger.error { "unable to parse messages $requestDebugInfo - timed out after $responseTimeout milliseconds" }
//                            else -> {
//                                logger.error(e) { "exception when send message $requestDebugInfo" }
//                                val rootCause = ExceptionUtils.getRootCause(e)
//                                requests.forEach { request -> request?.let { it.exception = rootCause } }
//                            }
//                        }
//
//                        requests.onEach { request ->
//                            request?.let { decodeRequests.remove(it.id) }
//                        }.forEach {
//                            try {
//                                it?.sendMessage(null)
//                            } catch (e: CancellationException) {
//                                logger.error { "cancelled channel from message: '${it?.rawMessage?.metadata?.id}'" }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    private suspend fun decodeMessage() {
        coroutineScope {
            while (true) {
                val batches = messageBatchBuffer.receive().filter { it.context.isActive }

                if (batches.isEmpty()) continue

                val requests = batches.flatMap { it.requests }.filterNotNull()
                val requestDebugInfo = getRequestDebugInfo(requests)
                val requestsMaps = requests.groupBy { it.id }

                try {
                    logMetrics(rabbitMqBatchParseMetrics) {

                        val batch = mergeBatches(requests)
                        sendMessageBatchToRabbit(batch, requestsMaps)

                        withTimeout(responseTimeout) {
                            requests.map { async { it.get() } }.awaitAll()
                                .also { logger.debug { "codec response received $requestDebugInfo" } }
                        }
                    }
                } catch (e: Exception) {
                    withContext(NonCancellable) {
                        when (e) {
                            is TimeoutCancellationException ->
                                logger.error { "unable to parse messages $requestDebugInfo - timed out after $responseTimeout milliseconds" }
                            else -> {
                                logger.error(e) { "exception when send message $requestDebugInfo" }
                                val rootCause = ExceptionUtils.getRootCause(e)
                                requests.forEach { request -> request.let { it.exception = rootCause } }
                            }
                        }

                        requests.onEach { request ->
                            request.let { decodeRequests.remove(it.id) }
                        }.forEach {
                            try {
                                it.sendMessage(null)
                            } catch (e: CancellationException) {
                                logger.error { "cancelled channel from message: '${it.rawMessage.metadata?.id}'" }
                            }
                        }
                    }
                }
            }
        }
    }


//    suspend fun decodeBatch(batchRequest: BatchRequest) {
//        // messageBuffer.send(batchRequest)
//    }

    suspend fun decodeBatch(batchRequest: List<BatchRequest>) {
        messageBatchBuffer.send(batchRequest)
    }

    private suspend fun sendMessageBatchToRabbit(
        batch: RawMessageBatch,
        requests: Map<MessageID, List<MessageRequest>>
    ) {
        withContext(mqDispatcher) {

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
                    messageRouterRawBatch.sendAll(batch, firstId?.connectionId?.sessionAlias)
                    logger.debug { "codec request published $requestDebugInfo" }
                } catch (e: IOException) {
                    logger.error(e) { "cannot send message $requestDebugInfo" }
                }
            }
        }
    }
}

