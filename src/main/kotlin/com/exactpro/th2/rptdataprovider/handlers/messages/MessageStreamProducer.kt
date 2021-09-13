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
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.takeWhile

class MessageStreamProducer private constructor(
    private val request: SseMessageSearchRequest,
    context: Context,
    private val messageBaskets: List<MessagesBasket>,
    private val basketInitializer: BasketInitializer
) {

    private val sseSearchDelay = context.configuration.sseSearchDelay.value.toLong()

    companion object {
        suspend fun create(
            request: SseMessageSearchRequest,
            context: Context,
            coroutineScope: CoroutineScope
        ): MessageStreamProducer {

            val streamInfo = BasketCreator(context, coroutineScope)
                .initStreamsInfo(request)

            val basketInitializer =
                BasketInitializer(context, coroutineScope, request, streamInfo.searchLimit)

            return MessageStreamProducer(request, context, streamInfo.baskets, basketInitializer)
        }
    }


    private fun isSearchInFuture(data: MessageWrapper?): Boolean {
        return data == null && request.keepOpen && request.searchDirection == TimeRelation.AFTER
    }


    private suspend fun loadBaskets(messageBaskets: List<MessagesBasket>) {
        coroutineScope {
            messageBaskets
                .sortedBy { it.lastTimestamp }
                .onEach { it.loadBasket() }
        }
    }


    private fun timestampInRange(wrapper: MessageWrapper): Boolean {
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
    suspend fun getMessageStream(): Flow<MessageWrapper> {
        return coroutineScope {
            flow {
                var searchInFuture = false
                do {
                    val data = getNextMessage()
                    data?.let { emit(it) }

                    searchInFuture = searchInFuture || isSearchInFuture(data)

                    if (data == null && searchInFuture) {
                        delay(sseSearchDelay * 1000)
                        basketInitializer.tryToInitBasketsInFuture(messageBaskets)
                        loadBaskets(messageBaskets)
                    }
                } while (data != null || searchInFuture)
            }.takeWhile {
                timestampInRange(it)
            }
        }
    }


    private suspend fun selectMessage(comparator: (MessageWrapper, MessageWrapper) -> Boolean): MessagesBasket? {
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
            resultElement
        }
    }

    private fun isLess(first: MessageWrapper, second: MessageWrapper): Boolean {
        return first.message.timestamp.isBefore(second.message.timestamp)
    }

    private fun isGreater(first: MessageWrapper, second: MessageWrapper): Boolean {
        return first.message.timestamp.isAfter(second.message.timestamp)
    }


    private suspend fun getNextMessage(): MessageWrapper? {
        var resultElement: MessageWrapper? = null
        do {
            resultElement = selectBasketWithNextMessage()?.top()

            val isDayChange = basketInitializer.isDayChanged(resultElement)

            basketInitializer.dayChangedInitBaskets(resultElement, messageBaskets)

        } while (isDayChange)

        return resultElement
    }

    private suspend fun selectBasketWithNextMessage(): MessagesBasket? {
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