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
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.cache.CodecCacheBatches
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import com.exactpro.th2.rptdataprovider.server.ServerType
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.google.protobuf.InvalidProtocolBufferException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging

class MessageProducer(
    private val serverType: ServerType,
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService,
    private val codecCache: CodecCache,
    private val codecCacheBatches: CodecCacheBatches
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

    private suspend fun createMessageBatch(
        messageBatch: StoredMessageBatch,
        processed: List<MessageRequest>?,
        parsedRawMessage: List<RawMessage?>,
        attachedEvents: List<Set<String>>?,
        needAttachEvents: Boolean
    ): ParsedMessageBatch {

        var allMessageParsed = true

        return ParsedMessageBatch(messageBatch.id,
            messageBatch.messages.mapIndexed { i, rawMessage ->
                val parsedMessage = processed?.get(i)?.get()
                Message(rawMessage, parsedMessage, parsedRawMessage[i]?.body, attachedEvents?.get(i)).also {
                    if (it.messageBody != null && it.rawMessageBody != null) {
                        codecCache.put(it.messageId, it)
                    } else {
                        allMessageParsed = false
                    }
                }
            }.associateBy { it.id },
            needAttachEvents
        ).also {
            if (allMessageParsed) {
                codecCacheBatches.put(it.id.toString(), it)
            }
        }
    }

    suspend fun fromRawMessage(
        messageBatch: StoredMessageBatch,
        searchRequest: SseMessageSearchRequest
    ): ParsedMessageBatch {
        return parseRawMessageBatch(messageBatch, searchRequest.attachedEvents)
    }

    private suspend fun parseRawMessageBatch(
        messageBatch: StoredMessageBatch,
        needAttachEvents: Boolean = true
    ): ParsedMessageBatch {
        return coroutineScope {

            codecCacheBatches.get(messageBatch.id.toString())?.let {
                if (!needAttachEvents || it.attachedEvents)
                    return@coroutineScope it
            }

            val parsedRawMessage = messageBatch.messages.map { parseRawMessage(it) }
            val parsedRawMessageProtocol = parsedRawMessage.firstOrNull()?.let { getFieldName(it) }

            val processed: List<MessageRequest>? =
                if (!isImage(parsedRawMessageProtocol)) parseMessage(messageBatch) else null

            val attachedEvents: List<Set<String>>? =
                if (needAttachEvents) getAttachedEvents(messageBatch.messages) else null

            return@coroutineScope createMessageBatch(
                messageBatch,
                processed,
                parsedRawMessage,
                attachedEvents,
                needAttachEvents
            )
        }
    }

    private suspend fun getAttachedEvents(
        messages: MutableCollection<StoredMessage>
    ): List<Set<String>> {
        return coroutineScope {
            messages.map { message ->
                async {
                    message.id.let {
                        try {
                            cradle.getEventIdsSuspend(it).map(Any::toString).toSet()
                        } catch (e: Exception) {
                            logger.error(e) { "unable to get events attached to message (id=${message.id})" }

                            emptySet<String>()
                        }
                    }
                }
            }.awaitAll()
        }
    }


    private suspend fun parseMessage(batch: StoredMessageBatch): List<MessageRequest>? {

        if (batch.isEmpty) {
            logger.error { "unable to parse message '${batch.id}' - message batch does not exist or is empty" }
            return null
        }
        return coroutineScope {
            rabbitMqService.decodeBatch(batch).toList().let {
                if (it.isEmpty()) {
                    logger.error { "Decoded batch can not be empty. Batch: ${batch.id}" }
                    null
                } else {
                    it
                }
            }
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
        ).firstOrNull()?.let { if (it.isEmpty) null else it }

        return rawBatchNullable?.let { rawBatch ->
            (codecCacheBatches.get(rawBatch.id.toString()) ?: parseRawMessageBatch(rawBatch)).batch[id]
        } ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}

