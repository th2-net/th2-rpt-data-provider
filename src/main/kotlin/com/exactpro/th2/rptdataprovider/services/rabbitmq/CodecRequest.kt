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

import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessageBatch
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import java.time.Instant


data class CodecId(private val ids: Set<String>) {

    companion object {
        fun fromRawBatch(groupBatch: MessageGroupBatch): CodecId {
            return CodecId(
                groupBatch.groupsList
                    .flatMap { group -> group.messagesList.map { it.rawMessage.metadata.id } }
            )
        }

        fun fromParsedBatch(groupBatch: MessageGroupBatch): CodecId {
            return CodecId(
                groupBatch.groupsList
                    .flatMap { group -> group.messagesList.map { it.message.metadata.id } }
            )
        }
    }

    constructor(ids: List<MessageID>) : this(
        ids
            .map { "${it.connectionId.sessionAlias}:${it.direction}:${it.sequence}" }
            .toSet()
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CodecId

        if (ids != other.ids) return false

        return true
    }

    override fun hashCode(): Int {
        return ids.hashCode()
    }
}


class CodecBatchRequest(
    val protobufRawMessageBatch: MessageGroupBatch
) {

    val id = CodecId.fromRawBatch(protobufRawMessageBatch)

    fun toPending(): PendingCodecBatchRequest {
        return PendingCodecBatchRequest(CompletableDeferred())
    }
}

class MessageGroupBatchWrapper(
    val messageGroupBatch: MessageGroupBatch
) {
    val id = CodecId.fromParsedBatch(messageGroupBatch)
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


