/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.rptdataprovider.producers

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.rptdataprovider.ProtoMessage
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import com.exactpro.th2.rptdataprovider.TransportRawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.isImage
import com.exactpro.th2.rptdataprovider.entities.internal.TransportBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.mappers.ProtoMessageMapper
import com.exactpro.th2.rptdataprovider.entities.mappers.TransportMessageMapper
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest.Companion.protocol
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest.Companion.protocol
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

interface MessageProducer<RM, PM> {
    suspend fun fromId(id: StoredMessageId): Message<RM, PM>

    companion object {
        internal const val SINGLE_REQUEST = "single_request"
    }
}

class ProtoMessageProducer(
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService<MessageGroupBatch, ProtoMessageGroup, ProtoMessage>
): MessageProducer<ProtoRawMessage, ProtoMessage> {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun fromId(id: StoredMessageId): Message<ProtoRawMessage, ProtoMessage> {

        return cradle.getMessageSuspend(id)?.let { stored ->
            val rawMessage: RawMessage = ProtoMessageMapper.storedMessageToRawProto(stored)
            val content: MessageGroupBatch = measureTimedValue {
                MessageGroupBatch
                    .newBuilder()
                    .addGroups(
                        MessageGroup
                            .newBuilder()
                            .addMessages(
                                AnyMessage
                                    .newBuilder()
                                    .setRawMessage(rawMessage)
                                    .build()
                            ).build()
                    ).build()
            }.also { logger.trace { "Grouping message $id ${it.duration}" } }.value

            var message: Message<ProtoRawMessage, ProtoMessage>? = null
            measureTimeMillis {
                val protocol = content.protocol()
                val decoded: List<BodyWrapper<ProtoMessage>>? = if (!isImage(protocol)) {
                    rabbitMqService.sendToCodec(
                        ProtoCodecBatchRequest(content, MessageProducer.SINGLE_REQUEST)
                    ).parsedMessageBatch
                        .await()
                        ?.findMessage(id.sequence)
                        ?.map { ProtoBodyWrapper(it) }
                } else null

                message = Message(MessageWrapper(stored, rawMessage.sessionGroup, rawMessage), decoded, setOf(), protocol)
            }.also { logger.trace { "Decoded message $id ${it}ms" } }
            message
        }

            ?: throw CradleMessageNotFoundException(id.toString())
    }
}

class TransportMessageProducer(
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService<GroupBatch, TransportMessageGroup, ParsedMessage>
): MessageProducer<TransportRawMessage, ParsedMessage> {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun fromId(id: StoredMessageId): Message<TransportRawMessage, ParsedMessage> {

        return cradle.getMessageSuspend(id)?.let { stored ->
            val rawMessage: TransportRawMessage = TransportMessageMapper.storedMessageToRawTransport(stored)
            val content: GroupBatch = measureTimedValue {
                rawMessage.toGroup().toBatch(stored.bookId.name, "")
            }.also { logger.trace { "Grouping message $id ${it.duration}" } }.value

            var message: Message<TransportRawMessage, ParsedMessage>? = null
            measureTimeMillis {
                val protocol = content.protocol()
                val decoded: List<BodyWrapper<ParsedMessage>>? = if (!isImage(protocol)) {
                    rabbitMqService.sendToCodec(
                        TransportCodecBatchRequest(content, MessageProducer.SINGLE_REQUEST)
                    ).parsedMessageBatch
                        .await()
                        ?.findMessage(id.sequence)
                        ?.map { TransportBodyWrapper(it, stored.bookId.name, "") } //FIXME: pass session group
                } else null

                message = Message(MessageWrapper(stored, "", rawMessage), decoded, setOf(), protocol) //FIXME: pass session group
            }.also { logger.trace { "Decoded message $id ${it}ms" } }
            message
        }

            ?: throw CradleMessageNotFoundException(id.toString())
    }
}