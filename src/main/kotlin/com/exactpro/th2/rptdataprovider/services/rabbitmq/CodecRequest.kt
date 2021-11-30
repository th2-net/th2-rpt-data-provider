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

import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.producers.BuildersBatch
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.*

data class MessageRequest(
    val id: MessageID,
    val rawMessage: RawMessage,
    private val channel: Channel<List<BodyWrapper>?>,
    private val result: Deferred<List<BodyWrapper>?>,
    private val requestId: UUID = UUID.randomUUID(),
    var exception: Throwable? = null
) : Comparable<MessageRequest> {

    var messageIsSend: Boolean = false
        private set

    companion object {
        suspend fun build(rawMessage: RawMessage): MessageRequest {
            val messageChannel = Channel<List<BodyWrapper>?>(0)
            return MessageRequest(
                id = rawMessage.metadata.id,
                rawMessage = rawMessage,
                channel = messageChannel,
                result = CoroutineScope(Dispatchers.Default).async {
                    messageChannel.receive()
                })
        }
    }

    suspend fun sendMessage(message: List<BodyWrapper>?) {
        if (result.isActive && !messageIsSend) {
            CoroutineScope(Dispatchers.Default).launch {
                channel.send(message)
            }
        }
        messageIsSend = true
    }

    suspend fun get(): List<BodyWrapper>? {
        val message = result.await()
        exception?.let { throw it }
        return message
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageRequest

        if (requestId != other.requestId) return false

        return true
    }

    override fun hashCode(): Int {
        return requestId.hashCode()
    }

    override fun compareTo(other: MessageRequest): Int {
        return requestId.compareTo(other.requestId)
    }
}


data class BatchRequest(
    val batchId: StoredMessageId,
    val messagesCount: Int,
    val requests: List<MessageRequest?>,
    val context: CoroutineScope
) {
    constructor(batch: BuildersBatch, requests: List<MessageRequest?>, context: CoroutineScope) : this(
        batchId = batch.batchId,
        messagesCount = batch.messageCount,
        requests = requests,
        context = context
    )
}