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
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import io.prometheus.client.Counter
import mu.KotlinLogging
import java.util.*

class MessageProducer(
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService,
    private val codecCache: CodecCache,
    private val useGetProcessedMessage: Boolean
) {

    companion object {
        private val logger = KotlinLogging.logger { }

        private val duplicateCounter =
            Counter.build("duplicate_counter", "Count of get message batch duplicates").register()

        private val parseMessageFailed =
            Counter.build("parse_message_failed", "Count of not parsed messages").register()
    }

    suspend fun fromRawMessage(rawMessage: StoredMessage, parsingOff: Boolean = false): Message {
        val processed = if (parsingOff) null else {
            if (useGetProcessedMessage) {
                cradle.getProcessedMessageSuspend(rawMessage.id)?.let { storedMessage ->
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
                }
            } else {
                null
            } ?: parseMessage(rawMessage)
        }

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

    private val messageBatchSet = mutableSetOf<String>()

    private suspend fun parseMessage(message: StoredMessage): com.exactpro.th2.common.grpc.Message? {
        return codecCache.get(message.id.toString())
            ?: let {
                val messages = cradle.getMessageBatchSuspend(message.id).also { msg ->
                    var isDuplicate = false
                    msg.forEach { m ->
                        if (!messageBatchSet.contains(m.id.toString()))
                            messageBatchSet.add(m.id.toString())
                        else
                            isDuplicate = true
                    }
                    if (isDuplicate) {
                        duplicateCounter.inc()
                    }
                }
                if (messages.isEmpty()) {
                    logger.error { "unable to parse message '${message.id}' - message batch does not exist or is empty" }
                    parseMessageFailed.inc()
                    return null
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
                    logger.error(e) { "unable to parse message '${message.id}'" }
                    parseMessageFailed.inc()
                    null
                } ?: let {
                    logger.error { "unable to parse message '${message.id}'" }
                    parseMessageFailed.inc()
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
                ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
        )
    }
}
