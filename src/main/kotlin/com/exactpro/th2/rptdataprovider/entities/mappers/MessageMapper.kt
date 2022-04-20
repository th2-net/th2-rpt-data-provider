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
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.FilteredMessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.exactpro.th2.rptdataprovider.providerDirectionToCradle
import java.util.*

object MessageMapper {


    private fun wrapSubMessage(message: BodyWrapper, matched: Boolean): MessageBodyWrapper {
        return MessageBodyWrapper.newBuilder()
            .setMessage(message.message)
            .setMatch(matched)
            .build()
    }

    private fun wrapSubMessages(filteredMessageWrapper: FilteredMessageWrapper): List<MessageBodyWrapper> {
        return with(filteredMessageWrapper) {
            message.parsedMessageGroup?.let { subMessages ->
                (subMessages zip filteredBody).map { (subMessage, matched) ->
                    wrapSubMessage(subMessage, matched)
                }
            } ?: emptyList()
        }
    }

    suspend fun convertToGrpcMessageData(filteredMessageWrapper: FilteredMessageWrapper): MessageData {
        return with(filteredMessageWrapper) {
            MessageSearchResponse.newBuilder()
                .setMessageId(message.id.convertToProto())
                .setTimestamp(message.timestamp.toTimestamp())
                .setBodyRaw(message.rawMessageBody)
                .addAllAttachedEventIds(message.attachedEventIds.map { EventID.newBuilder().setId(it).build() })
                .addAllMessages(wrapSubMessages(filteredMessageWrapper))
                .build()
        }
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
