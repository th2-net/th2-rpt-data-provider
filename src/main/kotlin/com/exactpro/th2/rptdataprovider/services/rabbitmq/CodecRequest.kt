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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import kotlinx.coroutines.CompletableDeferred


data class CodecId(private val ids: Set<BaseMessageId>) {

    data class BaseMessageId(val sessionAlias: String, val direction: Direction, val sequence: Long) {
        constructor(messageId: MessageID) : this(
            sessionAlias = messageId.connectionId.sessionAlias,
            direction = messageId.direction,
            sequence = messageId.sequence
        )
    }

    companion object {
        fun fromRawBatch(groupBatch: MessageGroupBatch): CodecId {
            return CodecId(
                groupBatch.groupsList
                    .flatMap { group -> group.messagesList.map { BaseMessageId(it.rawMessage.metadata.id) } }
                    .toSet()
            )
        }

        fun fromParsedBatch(groupBatch: MessageGroupBatch): CodecId {
            return CodecId(
                groupBatch.groupsList
                    .flatMap { group -> group.messagesList.map { BaseMessageId(it.message.metadata.id) } }
                    .toSet()
            )
        }
    }
}


class CodecBatchRequest(
    val protobufRawMessageBatch: MessageGroupBatch
) {

    val requestHash = CodecId.fromRawBatch(protobufRawMessageBatch)

    fun toPending(): PendingCodecBatchRequest {
        return PendingCodecBatchRequest(CompletableDeferred())
    }
}

class MessageGroupBatchWrapper(
    val messageGroupBatch: MessageGroupBatch
) {
    val requestHash = CodecId.fromParsedBatch(messageGroupBatch)
}

class PendingCodecBatchRequest(
    val completableDeferred: CompletableDeferred<MessageGroupBatchWrapper?>
) {
    fun toResponse(): CodecBatchResponse {
        return CodecBatchResponse(completableDeferred)
    }
}

class CodecBatchResponse(
    val protobufParsedMessageBatch: CompletableDeferred<MessageGroupBatchWrapper?>
)

