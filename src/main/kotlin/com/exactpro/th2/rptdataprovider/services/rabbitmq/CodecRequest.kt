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


data class CodecRequestId(private val ids: Set<BaseMessageId>) {

    data class BaseMessageId(val sessionAlias: String, val direction: Direction, val sequence: Long) {
        constructor(messageId: MessageID) : this(
            sessionAlias = messageId.connectionId.sessionAlias,
            direction = messageId.direction,
            sequence = messageId.sequence
        )

        override fun toString(): String {
            return "$sessionAlias:$direction:$sequence"
        }

    }

    companion object {
        fun fromMessageGroupBatch(groupBatch: MessageGroupBatch): CodecRequestId {
            return CodecRequestId(
                groupBatch.groupsList
                    .flatMap { group ->
                        group.messagesList.map {
                            if (it.hasMessage()) {
                                BaseMessageId(it.message.metadata.id)
                            } else {
                                BaseMessageId(it.rawMessage.metadata.id)
                            }
                        }
                    }
                    .toSet()
            )
        }
    }
}


class CodecBatchRequest(
    val protobufRawMessageBatch: MessageGroupBatch,
    val streamName: String
) {
    val requestId = CodecRequestId.fromMessageGroupBatch(protobufRawMessageBatch)
    val requestHash = requestId.hashCode()

    fun toPending(): PendingCodecBatchRequest {
        return PendingCodecBatchRequest(CompletableDeferred(), streamName)
    }
}

class MessageGroupBatchWrapper(
    val messageGroupBatch: MessageGroupBatch
) {
    val requestId = CodecRequestId.fromMessageGroupBatch(messageGroupBatch)
    val responseHash = requestId.hashCode()
}

class PendingCodecBatchRequest(
    val completableDeferred: CompletableDeferred<MessageGroupBatchWrapper?>,
    val streamName: String
) {
    val startTimestamp = System.currentTimeMillis()

    fun toResponse(): CodecBatchResponse {
        return CodecBatchResponse(completableDeferred)
    }
}

class CodecBatchResponse(
    val protobufParsedMessageBatch: CompletableDeferred<MessageGroupBatchWrapper?>
)

