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
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.time.Instant

data class MessageBatch(
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val id: StoredMessageId,
    val batch: Collection<StoredMessage>
) {
    companion object {
        fun build(batch: Collection<StoredMessage>): MessageBatch? {
            return if (batch.isNotEmpty()) {
                MessageBatch(
                    startTimestamp = batch.first().timestamp,
                    endTimestamp = batch.last().timestamp,
                    id = batch.first().id,
                    batch = batch
                )
            } else {
                null
            }
        }
    }
}

data class ParsedMessageBatch(
    val id: StoredMessageId,
    val batch: Collection<Message>
)