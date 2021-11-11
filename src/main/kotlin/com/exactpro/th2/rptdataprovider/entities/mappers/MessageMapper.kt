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

import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.FilteredMessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.BodyHttpMessage
import com.exactpro.th2.rptdataprovider.entities.responses.HttpBodyWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*

object MessageMapper {

    private val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .registerModule(KotlinModule())




    suspend fun convertToGrpcMessageData(filteredMessageWrapper: FilteredMessageWrapper): MessageData {
        return with(filteredMessageWrapper) {
            MessageData.newBuilder()
                .setMessageId(message.id.convertToProto())
                .setTimestamp(message.timestamp.toTimestamp())
                .setBodyRaw(message.rawMessageBody)
                .setAttachedEventIds(message.attachedEventIds.map { it. })
                .setBody(jacksonMapper.writeValueAsString(getBodyMessage(filteredMessageWrapper)))
                .build()
        }
    }

    suspend fun convertToHttpMessage(filteredMessageWrapper: FilteredMessageWrapper): HttpMessage {
        return with(filteredMessageWrapper) {
            val httpMessage = HttpMessage(
                timestamp = message.timestamp,
                direction = message.direction,
                sessionId = message.sessionId,
                attachedEventIds = message.attachedEventIds,
                messageId = message.id.toString(),
                body = getBodyMessage(filteredMessageWrapper),
                bodyBase64 = message.rawMessageBody?.let { Base64.getEncoder().encodeToString(it.toByteArray()) }
            )
            httpMessage
        }
    }
}