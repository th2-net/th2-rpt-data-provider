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
import com.exactpro.th2.common.grpc.RawMessageBatch
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred


class CodecBatchRequest(
    val protobufRawMessageBatch: RawMessageBatch
) {
    val requestHash = protobufRawMessageBatch.messagesList.map { it.metadata.id.hashCode() }.hashCode()

    fun toPending(): PendingCodecBatchRequest {
        return PendingCodecBatchRequest(CompletableDeferred())
    }

    //FIXME: find a better way to identify individual requests
    companion object {
        fun calculateHash(list: List<MessageGroup>): Int {
            return list.map { it.messagesList.first().message.metadata.id.hashCode() }.hashCode()
        }
    }
}

class PendingCodecBatchRequest(
    val completableDeferred: CompletableDeferred<MessageGroupBatch?>
) {
    fun toResponse(): CodecBatchResponse {
        return CodecBatchResponse(completableDeferred)
    }
}

class CodecBatchResponse(
    val protobufParsedMessageBatch: CompletableDeferred<MessageGroupBatch?>
)
