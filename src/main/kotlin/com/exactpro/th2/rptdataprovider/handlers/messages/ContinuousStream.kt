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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.MessagePipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.internal.RawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.lang.Integer.*
import java.time.Instant
import java.util.*


class ContinuousStream(
    private val context: Context,
    val stream: Pair<String, Direction>,
    val startMessageId: StoredMessageId?,
    val startTimestamp: Instant,
    private val request: SseMessageSearchRequest,
    private val externalScope: CoroutineScope,
    private var firstPull: Boolean = true
) : PipelineComponent<MessagePipelineStepObject, RawBatch>(externalScope) {

    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = min(maxMessagesLimit, request.resultCountLimit ?: 25)
    private var isStreamEmpty: Boolean = false
    private var lastElement: StoredMessageId? = startMessageId
    var lastTimestamp: Instant = startTimestamp
        private set

    private val messageStream: LinkedList<StoredMessageBatch> = LinkedList()
    private val messageFlow: Flow<StoredMessageBatch> = flow {
        messageStream.addAll(loadMoreMessage())
    }


    private fun changeStreamMessageIndex(filteredIdsList: List<StoredMessageBatch>): Pair<StoredMessageId?, Instant> {
        val lastBatch = filteredIdsList.lastOrNull { !it.isEmpty }
        return if (lastBatch == null) {
            lastElement to lastTimestamp
        } else {
            val lastMessage = if (request.searchDirection == TimeRelation.AFTER) {
                lastBatch.firstMessage
            } else {
                lastBatch.lastMessage
            }
            lastMessage.id to lastMessage.timestamp
        }
    }


    private suspend fun loadMoreMessage(): List<StoredMessageBatch> {
        if (lastElement == null) return emptyList()

        return context.messageStreamGenerator.pullMoreMessage(
            lastElement!!,
            request.searchDirection,
            firstPull,
            perStreamLimit
        ).also { messages ->
            changeStreamMessageIndex(messages).let {
                lastElement = it.first
                lastTimestamp = it.second
            }
            isStreamEmpty = messages.size < perStreamLimit
            firstPull = false
        }
    }


    override suspend fun processMessage(): RawBatch {
        return if (isStreamEmpty) {
            RawBatch(true, null, lastElement, lastTimestamp, request.searchDirection)
        } else {
            RawBatch(false, messageStream.pollFirst(), lastElement, lastTimestamp, request.searchDirection)
        }
    }
}


