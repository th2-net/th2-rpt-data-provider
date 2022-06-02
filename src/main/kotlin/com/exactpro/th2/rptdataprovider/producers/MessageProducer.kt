/*******************************************************************************
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.getProtocolField
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.isImage
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.services.AbstractDecoderService
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.CodecBatchRequest
import mu.KotlinLogging
import kotlin.system.measureTimeMillis

class MessageProducer(
    private val cradle: CradleService,
    private val decoderService: AbstractDecoderService
) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    suspend fun fromId(id: StoredMessageId): Message {

        return cradle.getMessageSuspend(id)?.let { stored ->
            val rawMessage: RawMessage = if (stored.content == null) {
                logger.error("Received stored message has no content. StoredMessageId: $id")
                RawMessage.newBuilder().build()
            } else
                RawMessage.parseFrom(stored.content)
            var content: MessageGroupBatch? = null
            measureTimeMillis {
                content = MessageGroupBatch
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
            }.also { logger.trace { "Grouping message $id ${it}ms" } }

            var message: Message? = null
            measureTimeMillis {
                val protocol = getProtocolField(content!!)
                val decoded = if (!isImage(protocol)) {
                    decoderService.sendToCodec(
                        CodecBatchRequest(content!!, "single_request")
                    )
                        .protobufParsedMessageBatch
                        .await()
                        ?.messageGroupBatch
                        ?.groupsList
                        ?.find { it.messagesList.first().message.metadata.id.sequence == id.index }
                        ?.messagesList
                        ?.map { BodyWrapper(it.message) }
                } else null

                message = Message(MessageWrapper(stored, rawMessage), decoded, setOf(), protocol)
            }.also { logger.trace { "Decoded message $id ${it}ms" } }
            message
        }

            ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}