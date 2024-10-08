/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.google.protobuf.ByteString
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.*

object ProtoMessageMapper {

    private val logger = KotlinLogging.logger { }

    fun storedMessageToRawProto(storedMessage: StoredMessage): RawMessage {
        return RawMessage.newBuilder()
            .setMetadata(
                RawMessageMetadata.newBuilder()
                    .setId(storedMessage.id.convertToProto())
                    .setProtocol(storedMessage.protocol)
                    .putAllProperties(storedMessage.metadata.toMap())
                    .build()
            ).also { builder ->
                storedMessage.content?.let {
                    builder.setBody(ByteString.copyFrom(it))
                } ?: logger.error { "Received stored message has no content. StoredMessageId: ${storedMessage.id}" }
            }
            .build()
    }


    //FIXME: migrate to grpc interface 1.0.0+
    //FIXME: return raw message body
    fun convertToGrpcMessageData(messageWithMetadata: MessageWithMetadata<RawMessage, Message>): List<MessageData> {
        return messageWithMetadata.message.parsedMessageGroup?.map { groupElement ->
            MessageData.newBuilder()
                .setMessageId(groupElement.id)
                .setTimestamp(groupElement.id.timestamp)
                .setMessageType(groupElement.messageType)
                .setMessage(groupElement.message)
                .build()
        } ?: listOf(messageWithMetadata.message.let { message ->
            MessageData.newBuilder()
                .setMessageId(message.id.convertToProto())
                .setTimestamp(message.timestamp.toTimestamp())
                .build()
        })
    }


    fun convertToHttpMessage(messageWithMetadata: MessageWithMetadata<RawMessage, Message>): HttpMessage {
        return with(messageWithMetadata) {
            HttpMessage(
                timestamp = message.timestamp,
                messageType = messageWithMetadata.message.parsedMessageGroup
                    ?.joinToString("/") { it.messageType } ?: messageWithMetadata.message.imageType ?: "",
                direction = message.direction,
                sessionId = message.sessionAlias,
                attachedEventIds = message.attachedEventIds,
                messageId = message.id.toString(),
                body = MessageMapper.getBodyMessage(messageWithMetadata),
                bodyBase64 = message.rawMessageBody.let { Base64.getEncoder().encodeToString(it) }
            )
        }
    }
}