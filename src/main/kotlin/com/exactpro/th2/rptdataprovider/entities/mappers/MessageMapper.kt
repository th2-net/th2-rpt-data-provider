/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.responses.HttpBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import java.util.*

object MessageMapper {

    private fun bodyWrapperToProto(bodyWrapper: BodyWrapper, isFiltered: Boolean = true): MessageBodyWrapper {
        return MessageBodyWrapper.newBuilder()
            .setMessage(bodyWrapper.message)
            .setFiltered(isFiltered)
            .build()
    }

    fun convertToGrpcMessageData(messageWithMetadata: MessageWithMetadata): MessageData {
        return with(messageWithMetadata) {
            MessageData.newBuilder()
                .setMessageId(message.id.convertToProto())
                .setTimestamp(message.timestamp.toTimestamp())
                .addAllAttachedEventIds(message.attachedEventIds.map { EventID.newBuilder().setId(it).build() })
                .setBodyRaw(message.rawMessageBody)
                .also { builder ->
                    message.messageBody?.let {
                        builder.addAllMessages(
                            it.zip(filteredBody).map { (msg, filtered) ->
                                bodyWrapperToProto(msg, filtered)
                            }
                        )
                    }
                }.build()
        }
    }

    suspend fun convertToHttpMessage(messageWithMetadata: MessageWithMetadata): HttpMessage {
        return with(messageWithMetadata) {
            HttpMessage(
                timestamp = message.timestamp,
                direction = message.direction,
                sessionId = message.sessionId,
                attachedEventIds = message.attachedEventIds,
                messageId = message.id.toString(),
                body = message.messageBody?.let {
                    it.zip(filteredBody).map { (msg, filtered) ->
                        HttpBodyWrapper.from(msg, filtered)
                    }
                },
                bodyBase64 = message.rawMessageBody?.let { Base64.getEncoder().encodeToString(it.toByteArray()) }
            )
        }
    }
}