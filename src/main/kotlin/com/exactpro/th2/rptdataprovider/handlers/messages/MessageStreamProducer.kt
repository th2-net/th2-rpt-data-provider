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
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class MessageStreamProducer private constructor(
    private val request: SseMessageSearchRequest,
    private val context: Context,
    private val messageBaskets: List<ContinuousStream>
) {

    private val sseSearchDelay = context.configuration.sseSearchDelay.value.toLong()

    companion object {
        suspend fun create(
            request: SseMessageSearchRequest,
            context: Context,
            coroutineScope: CoroutineScope
        ): MessageStreamProducer {
            val streamBasketProducer = BasketCreator(context, coroutineScope)
            return MessageStreamProducer(
                request,
                context,
                streamBasketProducer.initStreamsInfo(request)
            )
        }
    }


    private fun isSearchInFuture(data: MessageBatchWrapper?): Boolean {
        return data == null && request.keepOpen && request.searchDirection == TimeRelation.AFTER
    }


    private suspend fun loadBaskets(messageBaskets: List<MessagesBasket>) {
        coroutineScope {
            messageBaskets
                .sortedBy { it.lastTimestamp }
                .onEach { it.loadBasket() }
        }
    }


    private fun timestampInRange(wrapper: MessageBatchWrapper): Boolean {
        return wrapper.message.timestamp.let { timestamp ->
            if (request.searchDirection == TimeRelation.AFTER) {
                request.endTimestamp == null || timestamp.isBeforeOrEqual(request.endTimestamp)
            } else {
                request.endTimestamp == null || timestamp.isAfterOrEqual(request.endTimestamp)
            }
        }
    }


    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun getMessageStream(): Flow<MessageBatchWrapper> {
        return coroutineScope {
            flow {
                var searchInFuture = false
                do {
                    val data = getNextMessage()
                    data?.let { emit(it) }

                    searchInFuture = searchInFuture || isSearchInFuture(data)

                    if (data == null && searchInFuture) {
                        delay(sseSearchDelay * 1000)
                        loadBaskets(messageBaskets)
                    }
                } while (data != null || searchInFuture)
            }.takeWhile {
                timestampInRange(it)
            }
        }
    }


    private suspend fun selectMessage(comparator: (MessageBatchWrapper, MessageBatchWrapper) -> Boolean): MessageBatchWrapper? {
        return coroutineScope {
            var resultElement: MessagesBasket? = null
            for (basket in messageBaskets) {
                if (basket.top() != null) {
                    resultElement = when {
                        resultElement == null -> basket
                        comparator(basket.top()!!, resultElement.top()!!) -> basket
                        else -> resultElement
                    }
                }
            }
            resultElement?.pop()
        }
    }

    private fun isLess(first: MessageBatchWrapper, second: MessageBatchWrapper): Boolean {
        return first.message.timestamp.isBefore(second.message.timestamp)
    }

    private fun isGreater(first: MessageBatchWrapper, second: MessageBatchWrapper): Boolean {
        return first.message.timestamp.isAfter(second.message.timestamp)
    }

    private suspend fun getNextMessage(): MessageBatchWrapper? {
        return coroutineScope {
            if (request.searchDirection == TimeRelation.AFTER) {
                selectMessage { new, old ->
                    isLess(new, old)
                }
            } else {
                selectMessage { new, old ->
                    isGreater(new, old)
                }
            }
        }
    }

    fun getStreamsInfo(): List<StreamInfo> {
        return messageBaskets.map { it.getStreamInfo() }
    }
}