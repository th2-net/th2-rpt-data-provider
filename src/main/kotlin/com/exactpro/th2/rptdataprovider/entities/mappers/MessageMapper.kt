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
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.responses.BodyHttpMessage
import com.exactpro.th2.rptdataprovider.entities.responses.BodyHttpSubMessage
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


    private suspend fun getBodyMessage(messageWithMetadata: MessageWithMetadata): BodyHttpMessage? {
        return with(messageWithMetadata) {
            val body = message.messageBody?.let {
                it.zip(filteredBody).map { (msg, filtered) ->
                    HttpBodyWrapper.from(msg, filtered)
                }
            }
            val convertedBody = if (body != null) {
                if (body.size == 1) {
                    jacksonMapper.readValue<BodyHttpMessage>(body[0].message, BodyHttpMessage::class.java)
                } else {
                    mergeFieldsHttp(body)
                }
            } else null
            convertedBody
        }
    }

    suspend fun convertToGrpcMessageData(messageWithMetadata: MessageWithMetadata): MessageData {
        return with(messageWithMetadata) {
            MessageData.newBuilder()
                .setMessageId(message.id.convertToProto())
                .setTimestamp(message.timestamp.toTimestamp())
                .setBodyBase64(message.rawMessageBody?.let { Base64.getEncoder().encodeToString(it.toByteArray()) })
                .setBody(jacksonMapper.writeValueAsString(getBodyMessage(messageWithMetadata)))
                .build()
        }
    }

    suspend fun convertToHttpMessage(messageWithMetadata: MessageWithMetadata): HttpMessage {
        return with(messageWithMetadata) {
            val httpMessage = HttpMessage(
                timestamp = message.timestamp,
                messageType = messageWithMetadata.message.messageBody
                    ?.joinToString("/") { it.messageType } ?: messageWithMetadata.message.imageType ?: "",
                direction = message.direction,
                sessionId = message.sessionId,
                attachedEventIds = message.attachedEventIds,
                messageId = message.id.toString(),
                body = getBodyMessage(messageWithMetadata),
                bodyBase64 = message.rawMessageBody?.let { Base64.getEncoder().encodeToString(it.toByteArray()) }
            )
            httpMessage
        }
    }

    private fun mergeFieldsHttp(body: List<HttpBodyWrapper>): BodyHttpMessage {
        val res = jacksonMapper.readValue(body[0].message, BodyHttpMessage::class.java)
        res.fields = emptyMap<String, Any>().toMutableMap()
        body.map {
            it.subsequenceId.joinToString("-") to it
        }
            .sortedBy { it.first }
            .map {
                "${it.second.messageType}-${it.first}" to jacksonMapper.readValue(
                    it.second.message, BodyHttpMessage::class.java
                )
            }
            .forEach {
                res.fields?.set(
                    it.first, BodyHttpSubMessage(
                        mutableMapOf("fields" to (it.second.fields ?: mutableMapOf()))
                    )
                )
            }
        return res
    }
}