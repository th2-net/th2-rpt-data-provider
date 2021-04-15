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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatch
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
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


    suspend fun fromRawMessage(messageWrapper: MessageWrapper): MessageWrapper {
        codecCache.get(messageWrapper.id.toString())?.let {
            messageWrapper.setMessage(it)
            return messageWrapper
        }

        val parsedRawMessage = parseRawMessage(messageWrapper.message)
        val parsedRawMessageProtocol = getFieldName(parsedRawMessage)
        val processed = if (!isImage(parsedRawMessageProtocol)) parseMessage(messageWrapper.message) else null

        Message(
            messageWrapper.message,
            processed?.get()?.let { JsonFormat.printer().print(it) },
            parsedRawMessage?.let {
                Base64.getEncoder().encodeToString(it.body.toByteArray())
            },
            processed?.get()?.metadata?.messageType ?: parsedRawMessageProtocol ?: ""
        ).let {
            codecCache.put(it.messageId, it)
            messageWrapper.setMessage(it)
            return messageWrapper
        }
    }

    private suspend fun parseMessage(message: StoredMessage): MessageRequest {
        return rabbitMqService.decodeBatch(message)
    }


    suspend fun fromId(id: StoredMessageId): Message {
        return codecCache.get(id.toString())?.let {
            MessageWrapper(id, MessageBatch.build((cradle.getMessageBatchSuspend(id))))
        }?.let { messageBatch ->
            fromRawMessage(messageBatch).parsedMessage
        } ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}

