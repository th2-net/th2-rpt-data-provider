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

package com.exactpro.th2.rptdataprovider.producers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.entities.exceptions.ParseMessageException
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging
import java.util.*

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

    private suspend fun getProcessedMessage(rawMessage: StoredMessage): com.exactpro.th2.common.grpc.Message? {
        return cradle.getProcessedMessageSuspend(rawMessage.id)?.let { storedMessage ->
            storedMessage.content?.let {
                try {
                    com.exactpro.th2.common.grpc.Message.parseFrom(it)
                } catch (e: Exception) {
                    logger.error(e) {}
                    throw ParseMessageException(
                        "unable to parse message (id=${storedMessage.id}) " +
                                "- invalid data (${String(storedMessage.content)})"
                    )
                }
            }
        } ?: parseMessage(rawMessage)
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

    suspend fun fromRawMessage(rawMessage: StoredMessage): Message {
        return fromRawMessageWithInfo(rawMessage).first
    }

    private suspend fun catchProcessedMessageExceptions(rawMessage: StoredMessage):
            Pair<com.exactpro.th2.common.grpc.Message?, String?> {
        return try {
            Pair(getProcessedMessage(rawMessage), null)
        } catch (e: ParseMessageException) {
            Pair(null, e.message)
        }
    }

    suspend fun fromRawMessageWithInfo(rawMessage: StoredMessage): Pair<Message, String?> {
        val parsedRawMessage = parseRawMessage(rawMessage)
        val parsedRawMessageProtocol = getFieldName(parsedRawMessage)
        var error: String? = null
        val processed =
            if (isImage(parsedRawMessageProtocol)) {
                null
            } else {
                catchProcessedMessageExceptions(rawMessage).let {
                    error = it.second
                    it.first
                }
            }
        return Pair(
            Message(
                rawMessage,
                processed?.let { JsonFormat.printer().print(processed) },
                parsedRawMessage?.let {
                    Base64.getEncoder().encodeToString(it.body.toByteArray())
                },
                processed?.metadata?.messageType ?: parsedRawMessageProtocol ?: ""
            ), error
        )
    }

    private suspend fun parseMessage(message: StoredMessage): com.exactpro.th2.common.grpc.Message? {
        return codecCache.get(message.id.toString())
            ?: let {
                val messages = cradle.getMessageBatchSuspend(message.id)

                if (messages.isEmpty()) {
                    throw ParseMessageException("unable to parse message '${message.id}' - message batch does not exist or is empty")
                }

                val batch = RawMessageBatch.newBuilder().addAllMessages(
                    messages
                        .sortedBy { it.timestamp }
                        .map { RawMessage.parseFrom(it.content) }
                        .toList()
                ).build()

                try {
                    rabbitMqService.decodeMessage(batch)
                        .onEach { codecCache.put(getId(it.metadata.id).toString(), it) }
                        .firstOrNull { message.id == getId(it.metadata.id) }
                } catch (e: IllegalStateException) {
                    logger.error(e) { }
                    throw ParseMessageException("unable to parse message '${message.id}'")
                } ?: let {
                    throw ParseMessageException("unable to parse message '${message.id}'")
                }
            }
    }

    private fun getId(id: MessageID): StoredMessageId {
        val direction =
            if (id.direction == Direction.FIRST) com.exactpro.cradle.Direction.FIRST else com.exactpro.cradle.Direction.SECOND

        return StoredMessageId(id.connectionId.sessionAlias, direction, id.sequence)
    }

    suspend fun fromId(id: StoredMessageId): Message {

        return fromRawMessage(
            cradle.getMessageSuspend(id)
                ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
        )
    }
}
