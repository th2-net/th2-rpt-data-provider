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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import java.time.Instant


data class StreamBasket private constructor(
    private val context: Context,
    val stream: Pair<String, Direction>,
    val startMessageId: StoredMessageId?,
    val startTimestamp: Instant,
    private val request: SseMessageSearchRequest,
    private val coroutineScope: CoroutineScope
) {

    companion object {
        suspend fun create(
            context: Context,
            stream: Pair<String, Direction>,
            startMessageId: StoredMessageId?,
            startTimestamp: Instant,
            request: SseMessageSearchRequest,
            coroutineScope: CoroutineScope,
            firstPull: Boolean = true
        ): StreamBasket {
            return StreamBasket(context, stream, startMessageId, startTimestamp, request, coroutineScope).apply {
                isFirstPull = firstPull
                pop()            }
        }
    }


    var currentElement: MessageWrapper? = null
        private set

    var lastElement: StoredMessageId? = startMessageId
        private set

    var lastTimestamp: Instant? = startTimestamp
        private set

    var isStreamEmpty: Boolean = false
        private set

    var isFirstPull: Boolean = true
        private set


    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = Integer.min(maxMessagesLimit, request.resultCountLimit ?: 25)

    private val resumeIdStream =
        request.resumeFromId
            ?.let { StoredMessageId.fromString(it) }
            ?.let { Pair(it.streamName, it.direction) }

    private val streamProducer = StreamGenerator(context, coroutineScope)

    private var messageStream: Iterator<MessageWrapper> = emptyList<MessageWrapper>().iterator()

    private fun dropUntilInRangeInOppositeDirection(storedMessages: List<MessageWrapper>): List<MessageWrapper> {
        return if (request.searchDirection == TimeRelation.AFTER) {
            storedMessages.dropWhile { it.message.timestamp.isBefore(startTimestamp) }
        } else {
            storedMessages.dropWhile { it.message.timestamp.isAfter(startTimestamp) }
        }
    }


    private fun updateBasketState(size: Int, filteredIdsList: List<MessageWrapper>) {
        isStreamEmpty = size < perStreamLimit
        changeStreamMessageIndex(isStreamEmpty, filteredIdsList).let {
            lastElement = it.first
            lastTimestamp = it.second
        }
        isFirstPull = false
    }


    private fun changeStreamMessageIndex(
        streamEmpty: Boolean,
        filteredIdsList: List<MessageWrapper>
    ): Pair<StoredMessageId?, Instant?> {
        return if (request.searchDirection == TimeRelation.AFTER) {
            filteredIdsList.lastOrNull()?.let { it.id to it.message.timestamp }
                ?: if (request.keepOpen) {
                    lastElement to lastTimestamp
                } else {
                    null to null
                }
        } else {
            if (!streamEmpty) {
                val wrapper = filteredIdsList.firstOrNull()
                wrapper?.id to wrapper?.message?.timestamp
            } else {
                null to null
            }
        }
    }


    private suspend fun loadMoreMessage(): List<MessageWrapper> {
        return coroutineScope {
            val pulled = streamProducer.pullMoreSorted(lastElement, perStreamLimit, request, isFirstPull)

            dropUntilInRangeInOppositeDirection(pulled).let {
                updateBasketState(pulled.size, it)
                if (stream == resumeIdStream) {
                    it.filterNot { m -> m.message.id == startMessageId }
                } else {
                    it
                }
            }.also {
                perStreamLimit = Integer.min(maxMessagesLimit, perStreamLimit * 2)
            }
        }
    }


    private fun nextOrNull(): MessageWrapper? {
        return if (messageStream.hasNext()) {
            messageStream.next()
        } else {
            null
        }
    }

    private suspend fun autoUpdateBasket() {
        if (request.searchDirection == TimeRelation.AFTER) {
            if (request.keepOpen || !isStreamEmpty) {
                messageStream = loadMoreMessage().iterator()
            }
        } else {
            if (!isStreamEmpty) {
                messageStream = loadMoreMessage().iterator()
            }
        }
    }

    private suspend fun nextMessageInStream(): MessageWrapper? {
        if (!messageStream.hasNext()) {
            autoUpdateBasket()
        }
        return nextOrNull()
    }


    suspend fun loadBasket() {
        if (!messageStream.hasNext()) {
            messageStream = loadMoreMessage().iterator()
        }
        pop()
    }

    suspend fun pop(): MessageWrapper? {
        return coroutineScope {
            val oldMessage = currentElement
            currentElement = nextMessageInStream()
            oldMessage
        }
    }

    fun top(): MessageWrapper? {
        return currentElement
    }

    fun getStreamInfo(): StreamInfo {
        return StreamInfo(stream, currentElement?.id)
    }
}