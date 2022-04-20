﻿/*******************************************************************************
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
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.FilteredMessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.exactpro.th2.rptdataprovider.providerDirectionToCradle

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.protobuf.ByteString
import java.util.*

object MessageMapper {
    //FIXME: migrate to grpc interface 1.0.0+
    //FIXME: return raw message body
    fun convertToGrpcMessageData(messageWithMetadata: FilteredMessageWrapper): MessageGroupResponse {
        return MessageGroupResponse.newBuilder()
            .setMessageId(messageWithMetadata.message.id.convertToProto())
            .setTimestamp(messageWithMetadata.message.timestamp.toTimestamp())
//                .setBodyBase64(messageWithMetadata.message.rawMessageBody.let {
//                    Base64.getEncoder().encodeToString(it)
//                }
            .setBodyRaw(messageWithMetadata.message.rawMessageBody)
            .addAllAttachedEventId(messageWithMetadata.message.attachedEventIds.map {
                EventID.newBuilder().setId(it).build()
            })
            .also { builder ->
                messageWithMetadata.getMessagesWithMatches()?.let {
                    builder.addAllMessageItem(
                        it.map { (bodyWrapper, match) ->
                            MessageGroupItem.newBuilder()
                                .setMatch(match)
                                .setMessage(bodyWrapper.message)
                                .build()
                        }
                    )
                }
            }
            .build()
//        } ?: listOf(messageWithMetadata.message.let { message ->
//            MessageData.newBuilder()
//                .setMessageId(message.id.convertToProto())
//                .setTimestamp(message.timestamp.toTimestamp())
////                .setBodyBase64(message.rawMessageBody.let { Base64.getEncoder().encodeToString(it) })
//                .build()
//        })
    }

    suspend fun convertToHttpMessage(filteredMessageWrapper: FilteredMessageWrapper): HttpMessage {
        return with(filteredMessageWrapper) {
            val httpMessage = HttpMessage(
                id = message.messageId,
                timestamp = message.timestamp,
                sessionId = message.sessionId,
                direction = providerDirectionToCradle(message.direction),
                sequence = message.id.index.toString(),
                attachedEventIds = message.attachedEventIds,
                rawMessageBase64 = message.rawMessageBody?.let { Base64.getEncoder().encodeToString(it.toByteArray()) },
                parsedMessages = message.parsedMessageGroup?.let {
                    it.zip(filteredBody).map { (msg, filtered) ->
                        HttpBodyWrapper.from(msg, filtered)
                    }
                }
            )
            httpMessage
        }
    }
}
