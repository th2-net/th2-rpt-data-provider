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

package com.exactpro.th2.rptdataprovider.producers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.BatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.google.protobuf.InvalidProtocolBufferException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext


data class BuildersBatch(
    val builders: List<Message.Builder>,
    val rawMessages: List<RawMessage?>,
    val isImages: Boolean
)


class MessageProducer(
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService,
    private val codecCache: CodecCache
) {

    companion object {
        private val logger = KotlinLogging.logger { }
        private val messageProtocolDescriptor =
            RawMessage.getDescriptor().findFieldByName("metadata").messageType.findFieldByName("protocol")
        private val TYPE_IMAGE = "image"
    }


    private suspend fun parseRawMessage(rawMessage: StoredMessage): RawMessage? {
        return try {
            RawMessage.parseFrom(rawMessage.content)
        } catch (e: InvalidProtocolBufferException) {
            logger.error {
                "unable to unpack raw message '${rawMessage.id}' - invalid data '${String(rawMessage.content)}'"
            }
            null
        }
    }


    private suspend fun getFieldName(parsedRawMessage: RawMessage?): String? {
        return try {
            parsedRawMessage?.metadata?.getField(messageProtocolDescriptor).toString()
        } catch (e: Exception) {
            logger.error(e) { "Field: '${messageProtocolDescriptor.name}' does not exist in message: $parsedRawMessage " }
            null
        }
    }


    private fun isImage(protocolName: String?): Boolean {
        return protocolName?.contains(TYPE_IMAGE) ?: false
    }


    private fun createMessageBatch(
        messageBatch: MessageBatchWrapper,
        parsedRawMessage: List<RawMessage?>
    ): List<Message.Builder> {
        return messageBatch.messages.mapIndexed { i, rawMessage ->
            Message.Builder(rawMessage, parsedRawMessage[i])
        }
    }


    private suspend fun fullMessageParsing(messageBatch: MessageBatchWrapper): List<Message> {
        return coroutineScope {
            messageBatchToBuilders(messageBatch).let { messages ->
                val parsedMessages =
                    if (!messages.isImages) {
                        parseMessages(messageBatch, messages.rawMessages, this)
                    } else {
                        null
                    }

                attachEvents(messages.builders)

                messages.builders.mapIndexed { index, builder ->
                    builder.parsedMessage(parsedMessages?.get(index)?.get())
                    builder.build().also {
                        if (it.messageBody != null && it.rawMessageBody != null) {
                            codecCache.put(it.messageId, it)
                        }
                    }
                }
            }
        }
    }


    suspend fun attachEvents(builders: List<Message.Builder>) {
        coroutineScope {
            builders.map { builder ->
                async {
                    val eventsId = builder.rawStoredMessage.id.let {
                        try {
                            cradle.getEventIdsSuspend(it).map(Any::toString).toSet()
                        } catch (e: Exception) {
                            logger.error(e) { "unable to get events attached to message (id=$it)" }

                            emptySet<String>()
                        }
                    }
                    builder.attachedEvents(eventsId)
                }
            }.awaitAll()
        }
    }


    suspend fun parseMessages(
        batchWrapper: MessageBatchWrapper,
        parsedRawMessage: List<RawMessage?>,
        coroutineScope: CoroutineScope
    ): List<MessageRequest?> {

        if (batchWrapper.messageBatch.isEmpty) {
            logger.error { "unable to parse message '${batchWrapper.messageBatch.id}' - message batch does not exist or is empty" }
            return emptyList()
        }

        val messageRequests = parsedRawMessage.map { message -> message?.let { MessageRequest.build(it) } }

        val batchRequest = BatchRequest(batchWrapper.messageBatch, messageRequests, coroutineScope)

        rabbitMqService.decodeBatch(batchRequest)

        return messageRequests
    }


    suspend fun messageBatchToBuilders(messageBatch: MessageBatchWrapper): BuildersBatch {
        return coroutineScope {
            val parsedRawMessage = messageBatch.messages.map { parseRawMessage(it) }
            val parsedRawMessageProtocol = parsedRawMessage.firstOrNull()?.let { getFieldName(it) }
            val messageBuilders = createMessageBatch(messageBatch, parsedRawMessage)

            return@coroutineScope BuildersBatch(
                messageBuilders,
                parsedRawMessage,
                isImage(parsedRawMessageProtocol)
            )
        }
    }


    suspend fun fromId(id: StoredMessageId): Message {

        codecCache.get(id.toString())?.let { return it }

        val rawBatchNullable = cradle.getMessagesBatchesSuspend(
            StoredMessageFilterBuilder()
                .streamName().isEqualTo(id.streamName)
                .direction().isEqualTo(id.direction)
                .index().isEqualTo(id.index)
                .build()
        ).firstOrNull()?.takeIf { !it.isEmpty }

        return rawBatchNullable?.let { rawBatch ->

            val wrappedBatch = MessageBatchWrapper(rawBatch)

            fullMessageParsing(wrappedBatch).first { it.id == id }

        } ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}

