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
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.cache.CodecCacheBatches
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.util.*

class MessageProducer(
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

    suspend fun fromRawMessage(messageBatch: StoredMessageBatch): ParsedMessageBatch {
        return coroutineScope {
            codecCacheBatches.get(messageBatch.id.toString())?.let {
                return@coroutineScope it
            }

            val parsedRawMessage = messageBatch.messages.map { parseRawMessage(it) }
            val parsedRawMessageProtocol = parsedRawMessage.firstOrNull()?.let { getFieldName(it) }
            val processed: List<MessageRequest>? =
                if (!isImage(parsedRawMessageProtocol)) parseMessage(messageBatch) else null

            val errors = processed?.mapNotNull { it.exception }

            return@coroutineScope ParsedMessageBatch(messageBatch.id,
                messageBatch.messages.mapIndexed { i, rawMessage ->
                    Message(
                        rawMessage,
                        processed?.get(i)?.getMessage()?.let { JsonFormat.printer().print(it) },
                        parsedRawMessage[i]?.let {
                            Base64.getEncoder().encodeToString(it.body.toByteArray())
                        },
                        processed?.get(i)?.getMessage()?.metadata?.messageType ?: parsedRawMessageProtocol ?: ""
                    ).also { codecCache.put(it.messageId, it) }
                }.associateBy { it.id },
                errors
            ).also {
                if (errors?.isEmpty() == true) {
                    codecCacheBatches.put(it.id.toString(), it)
                }
            }
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
            (codecCacheBatches.get(rawBatch.id.toString()) ?: fromRawMessage(rawBatch)).batch[id]
        } ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}

