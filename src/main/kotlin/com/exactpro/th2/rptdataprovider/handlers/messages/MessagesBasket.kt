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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import java.lang.Integer.min
import java.time.Instant
import java.util.*


data class MessagesBasket private constructor(
    private val context: Context,
    val stream: Pair<String, Direction>,
    val startTimestamp: Instant,
    private var startMessageId: StoredMessageId?,
    private val request: SseMessageSearchRequest,
    private val streamProducer: StreamGenerator
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
        ): MessagesBasket {
            val streamGenerator =
                StreamGenerator.create(
                    context, coroutineScope,
                    request, startTimestamp, firstPull
                )

            return MessagesBasket(
                context, stream, startTimestamp,
                startMessageId, request, streamGenerator
            ).apply {
                loadBasket()
            }
        }
    }

    private var lastElement: StoredMessageId? = startMessageId

    var lastTimestamp: Instant? = startTimestamp
        private set

    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = min(maxMessagesLimit, request.resultCountLimit ?: 25)

    private val messageStream: LinkedList<MessageWrapper> = LinkedList()

    private fun changeStreamMessageIndex(filteredIdsList: List<MessageWrapper>): Pair<StoredMessageId?, Instant?> {
        val wrapper = filteredIdsList.lastOrNull()
        return wrapper?.let { it.id to it.message.timestamp } ?: lastElement to lastTimestamp
    }

    private suspend fun loadMoreMessage(): List<MessageWrapper> {
        return coroutineScope {
            streamProducer.pullMoreMessage(lastElement, perStreamLimit).also { messages ->
                changeStreamMessageIndex(messages).let {
                    lastElement = it.first
                    lastTimestamp = it.second
                }
                perStreamLimit = min(perStreamLimit * 2, maxMessagesLimit)
            }
        }
    }

    private suspend fun autoUpdateBasket() {
        if (messageStream.isEmpty() && !streamProducer.isStreamEmpty) {
            messageStream.addAll(loadMoreMessage())
        }
    }

    suspend fun pop(): MessageWrapper? {
        return coroutineScope {
            messageStream.pollFirst().apply {
                autoUpdateBasket()
            }
        }
    }

    fun top(): MessageWrapper? {
        return messageStream.peekFirst()
    }

    suspend fun loadBasket() {
        if (messageStream.isEmpty()) {
            messageStream.addAll(loadMoreMessage())
        }
    }

    fun getStreamInfo(): StreamInfo {
        return StreamInfo(stream, top()?.id, streamProducer.isStreamEmpty, startMessageId)
    }

    suspend fun tryToInitBasket(timestamp: Instant) {
        if (this.startMessageId == null) {
            val messageId = context.cradleService.getFirstMessageIdSuspend(
                timestamp,
                stream.first,
                stream.second,
                request.searchDirection
            )
            this.startMessageId = messageId
            this.lastElement = messageId
        }
    }
}