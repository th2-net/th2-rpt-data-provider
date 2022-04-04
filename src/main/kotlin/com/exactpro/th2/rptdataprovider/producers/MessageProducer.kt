/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.getProtocolField
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.isImage
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.services.cradle.CradleMessageNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService

class MessageProducer(
    private val cradle: CradleService,
    private val rabbitMqService: RabbitMqService
) {

    suspend fun fromId(id: StoredMessageId): Message {

        return cradle.getMessageSuspend(id)?.let { stored ->

            val content = MessageGroupBatch
                .newBuilder()
                .addGroups(
                    MessageGroup
                        .newBuilder()
                        .addMessages(
                            AnyMessage
                                .newBuilder()
                                .setRawMessage(RawMessage.parseFrom(stored.content))
                                .build()
                        ).build()
                ).build()


            val decoded = if (!isImage(getProtocolField(content))) {
                rabbitMqService.sendToCodec(
                    CodecBatchRequest(content, "single_request")
                )
                    .protobufParsedMessageBatch
                    .await()
                    ?.messageGroupBatch
                    ?.groupsList
                    ?.find { it.messagesList.first().message.metadata.id.sequence == id.index }
                    ?.messagesList
                    ?.map { BodyWrapper(it.message) }
            } else null

            Message(stored, decoded, stored.content, setOf())
        }

            ?: throw CradleMessageNotFoundException("message '${id}' does not exist in cradle")
    }
}

