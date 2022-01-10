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
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.PipelineStatus
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import mu.KotlinLogging
import java.time.Instant

class MessageLoader(
    private val context: Context,
    private val startTimestamp: Instant,
    private val searchDirection: TimeRelation,
    private val pipelineStatus: PipelineStatus
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val dbRetryDelay = context.configuration.dbRetryDelay.value.toLong()

    private suspend fun pullMore(
        startId: StoredMessageId,
        include: Boolean,
        limit: Int
    ): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=${searchDirection})" }

        return context.cradleService.getMessagesBatchesSuspend(
            StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(startId.streamName)
                direction().isEqualTo(startId.direction)
                limit(limit)

                if (searchDirection == AFTER) {
                    index().let {
                        if (include)
                            it.isGreaterThanOrEqualTo(startId.index)
                        else
                            it.isGreaterThan(startId.index)
                    }
                    order(Order.DIRECT)
                } else {
                    index().let {
                        if (include)
                            it.isLessThanOrEqualTo(startId.index)
                        else
                            it.isLessThan(startId.index)
                    }
                    order(Order.REVERSE)
                }
            }.build()
        )
    }


    private fun isLessThenStart(message: StoredMessage, include: Boolean, startId: StoredMessageId): Boolean {
        return message.timestamp.isBefore(startTimestamp) &&
                startId.let {
                    if (include) {
                        message.index <= it.index
                    } else {
                        message.index < it.index
                    }
                }
    }


    private fun isGreaterThenStart(message: StoredMessage, include: Boolean, startId: StoredMessageId): Boolean {
        return message.timestamp.isAfter(startTimestamp) &&
                startId.let {
                    if (include) {
                        message.index >= it.index
                    } else {
                        message.index > it.index
                    }
                }
    }


    private fun getFirstIdInRange(startId: StoredMessageId, include: Boolean, batch: StoredMessageBatch): Int? {
        val firstMessageInRange =
            if (searchDirection == AFTER) {
                batch.messages.indexOfFirst { isGreaterThenStart(it, include, startId) }
            } else {
                batch.messagesReverse.indexOfFirst { isLessThenStart(it, include, startId) }
            }

        return firstMessageInRange.takeIf { it != -1 }
    }


    suspend fun pullMoreMessage(startId: StoredMessageId, include: Boolean, limit: Int): List<MessageBatchWrapper> {
        return databaseRequestRetry(dbRetryDelay) {
            pullMore(startId, include, limit)
        }.mapNotNull { batch ->
            getFirstIdInRange(startId, include, batch)?.let {
                pipelineStatus.streams[startId.streamName + ":" +startId.direction.toString()]?.counters?.fetched?.incrementAndGet()
                MessageBatchWrapper(batch, it, searchDirection)
            }
        }
    }
}