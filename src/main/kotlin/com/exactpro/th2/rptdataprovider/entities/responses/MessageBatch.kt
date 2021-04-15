/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.time.Instant

data class MessageWrapper(
    val id: StoredMessageId,
    val message: StoredMessage,
    private val result: Deferred<ParsedMessageBatch>
) {
    companion object {
        suspend fun build(message: StoredMessage, parseMessageBatch: suspend () -> ParsedMessageBatch): MessageWrapper {
            return MessageWrapper(
                id = message.id,
                message = message,
                result = CoroutineScope(Dispatchers.Default).async {
                    parseMessageBatch.invoke()
                })
        }
    }

    suspend fun getParsedMessage(): Message {
        return result.await().batch.first { it.id == id }
    }
}


data class MessageBatch(
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val id: StoredMessageId,
    val batch: Collection<StoredMessage>
) {
    companion object {
        fun build(batch: Collection<StoredMessage>): MessageBatch {
            return MessageBatch(
                startTimestamp = batch.first().timestamp,
                endTimestamp = batch.last().timestamp,
                id = batch.first().id,
                batch = batch
            )
        }
    }
}


data class ParsedMessageBatch(
    val id: StoredMessageId,
    val batch: Collection<Message>
)

