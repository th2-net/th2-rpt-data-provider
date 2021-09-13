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

import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.time.Instant

class StreamGenerator(
    private val context: Context,
    private val startTimestamp: Instant,
    private val request: SseMessageSearchRequest,
    private val coroutineScope: CoroutineScope
) {

    companion object {
        private val logger = KotlinLogging.logger {}
        fun create(
            context: Context,
            coroutineScope: CoroutineScope,
            request: SseMessageSearchRequest,
            startTimestamp: Instant,
            firstPull: Boolean = true
        ): StreamGenerator {
            return StreamGenerator(context, startTimestamp, request, coroutineScope).apply {
                isFirstPull = firstPull
            }
        }
    }

    private val dbRetryDelay = context.configuration.dbRetryDelay.value.toLong()

    var isStreamEmpty: Boolean = false
        private set

    var isFirstPull: Boolean = true
        private set


    private suspend fun pullMore(startId: StoredMessageId?, limit: Int): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=${request.searchDirection})" }

        if (startId == null) return emptyList()

        return context.cradleService.getMessagesBatchesSuspend(
            StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(startId.streamName)
                direction().isEqualTo(startId.direction)
                limit(limit)

                if (request.searchDirection == AFTER) {
                    index().let {
                        if (isFirstPull)
                            it.isGreaterThanOrEqualTo(startId.index)
                        else
                            it.isGreaterThan(startId.index)
                    }
                    order(Order.DIRECT)
                } else {
                    index().let {
                        if (isFirstPull)
                            it.isLessThanOrEqualTo(startId.index)
                        else
                            it.isLessThan(startId.index)
                    }
                    order(Order.REVERSE)
                }
            }.build()
        )
    }


    private suspend fun pullMoreWrapped(startId: StoredMessageId?, limit: Int): List<MessageWrapper> {
        return coroutineScope {
            databaseRequestRetry(dbRetryDelay) {
                pullMore(startId, limit)
            }.map { batch ->
                async {
                    val parsedFuture = coroutineScope.async {
                        context.messageProducer.fromRawMessage(batch, request)
                    }
                    batch.messages.let {
                        if (request.searchDirection == AFTER) it else it.reversed()
                    }.map { message ->
                        MessageWrapper.build(message, parsedFuture)
                    }
                }
            }.awaitAll().also {
                isStreamEmpty = it.size < limit
            }.flatten()
        }
    }


    suspend fun pullMoreMessage(startId: StoredMessageId?, limit: Int): List<MessageWrapper> {
        return coroutineScope {
            pullMoreWrapped(startId, limit).let {
                dropUntilInRangeInOppositeDirection(startId, it)
            }.also {
                isFirstPull = false
            }
        }
    }


    private fun isLessThenStart(wrapper: MessageWrapper, startId: StoredMessageId?): Boolean {
        return wrapper.message.timestamp.isBefore(startTimestamp) ||
                startId?.let {
                    if (isFirstPull) {
                        wrapper.message.index < it.index
                    } else {
                        wrapper.message.index <= it.index
                    }
                } ?: false
    }

    private fun isGreaterThenStart(wrapper: MessageWrapper, startId: StoredMessageId?): Boolean {
        return wrapper.message.timestamp.isAfter(startTimestamp) ||
                startId?.let {
                    if (isFirstPull) {
                        wrapper.message.index > it.index
                    } else {
                        wrapper.message.index >= it.index
                    }
                } ?: false
    }


    private fun dropUntilInRangeInOppositeDirection(
        startId: StoredMessageId?,
        storedMessages: List<MessageWrapper>
    ): List<MessageWrapper> {
        return if (request.searchDirection == AFTER) {
            storedMessages.dropWhile { isLessThenStart(it, startId) }
        } else {
            storedMessages.dropWhile { isGreaterThenStart(it, startId) }
        }
    }
}