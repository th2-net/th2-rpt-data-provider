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
import com.exactpro.th2.rptdataprovider.entities.responses.Message
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
    }

    suspend fun fromRawMessage(rawMessage: StoredMessage): Message {
        val processed = cradle.getProcessedMessageSuspend(rawMessage.id)?.let { storedMessage ->

            storedMessage.content?.let {
                try {
                    com.exactpro.th2.common.grpc.Message.parseFrom(it)
                } catch (e: Exception) {
                    logger.error {
                        "unable to parse message (id=${storedMessage.id}) - invalid data (${String(storedMessage.content)})"
                    }

                    null
                }
            }
        } ?: parseMessage(rawMessage)

        return Message(
            rawMessage,
            processed?.let { JsonFormat.printer().print(processed) },

            rawMessage.content?.let {
                try {
                    RawMessage.parseFrom(it)?.body?.let { body ->
                        Base64.getEncoder().encodeToString(body.toByteArray())
                    }
                } catch (e: InvalidProtocolBufferException) {
                    logger.error {
                        "unable to unpack raw message '${rawMessage.id}' - invalid data '${String(rawMessage.content)}'"
                    }
                    null
                }
            },

            processed?.metadata?.messageType ?: ""
        )
    }

    private suspend fun parseMessage(message: StoredMessage): com.exactpro.th2.common.grpc.Message? {
        return codecCache.get(message.id.toString())
            ?: let {
                val messages = cradle.getMessageBatch(message.id)

                val batch = RawMessageBatch.newBuilder().addAllMessages(
                    messages
                        .sortedBy { it.timestamp }
                        .map { RawMessage.parseFrom(it.content) }
                        .toList()
                ).build()

                rabbitMqService.decodeMessage(batch)
                    .onEach { codecCache.put(getId(it.metadata.id).toString(), it) }
                    .firstOrNull { message.id == getId(it.metadata.id) }

                    ?: let {
                        logger.error { "unable to parse message '${message.id}' using RabbitMqService - parsed message is set to 'null'" }
                        null
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
                ?: throw IllegalArgumentException("message '${id}' does not exist in cradle")
        )
    }
}
