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

import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineFilteredMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineParsedMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.*
import java.lang.Integer.*
import java.sql.Timestamp
import java.time.Instant
import java.util.*


class MessageContinuousStream(
    private val messageLoader: MessageLoader,
    startMessageId: StoredMessageId?,
    initializer: StreamInitializer,
    startTimestamp: Instant,
    externalScope: CoroutineScope
) : PipelineComponent(initializer.context, initializer.request, initializer.stream, externalScope) {


    private val sseSearchDelay = context.configuration.sseSearchDelay.value.toLong()
    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = min(maxMessagesLimit, searchRequest.resultCountLimit ?: 25)

    private var firstPull: Boolean = startMessageId == null
    private var isStreamEmpty: Boolean = false

    private var lastElement: StoredMessageId? = startMessageId
    private var lastTimestamp: Instant = startTimestamp


    private fun changeStreamMessageIndex(filteredIdsList: List<MessageBatchWrapper>): Pair<StoredMessageId?, Instant> {
        val batchWrapper = filteredIdsList.lastOrNull { !it.messageBatch.isEmpty }
        return if (batchWrapper == null) {
            lastElement to lastTimestamp
        } else {
            val lastMessage =
                if (searchRequest.searchDirection == TimeRelation.AFTER) {
                    batchWrapper.messageBatch.firstMessage
                } else {
                    batchWrapper.messageBatch.lastMessage
                }
            lastMessage.id to lastMessage.timestamp
        }
    }


    private suspend fun loadMoreMessage(): List<MessageBatchWrapper> {
        if (lastElement == null) return emptyList()

        return messageLoader.pullMoreMessage(lastElement!!, firstPull, perStreamLimit).also { messages ->
            changeStreamMessageIndex(messages).let {
                lastElement = it.first
                lastTimestamp = it.second
            }
            isStreamEmpty = messages.size < perStreamLimit
            firstPull = false
        }
    }


    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            lastElement?.let {
                sendToChannel(EmptyPipelineObject(isStreamEmpty, lastElement, lastTimestamp))
                delay(sseSearchDelay)
            }
        }
    }


    override suspend fun processMessage() {
        coroutineScope {
            launch { emptySender(this) }
            while (isActive) {
                val messageBatches = loadMoreMessage()
                for (parsedMessage in messageBatches) {
                    sendToChannel(PipelineRawBatchData(isStreamEmpty, lastElement, lastTimestamp, parsedMessage))
                }
            }
        }
    }
}


