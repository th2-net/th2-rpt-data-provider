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
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import mu.KotlinLogging
import java.time.Instant

class MessageLoader(
    private val context: Context,
    private val streamName: StreamName,
    private val startTimestamp: Instant,
    private val request: SseMessageSearchRequest
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val dbRetryDelay = context.configuration.dbRetryDelay.value.toLong()


    private suspend fun pullMoreById(
        startId: StoredMessageId,
        include: Boolean,
        limit: Int
    ): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=${request.searchDirection})" }

        return context.cradleService.getMessagesBatchesSuspend(
            MessageFilterBuilder().apply {
                sessionAlias(startId.sessionAlias)
                direction(startId.direction)
                bookId(request.bookId)
                limit(limit)

                if (request.searchDirection == AFTER) {
                    sequence().let {
                        if (include)
                            it.isGreaterThanOrEqualTo(startId.sequence)
                        else
                            it.isGreaterThan(startId.sequence)
                    }
                    order(Order.DIRECT)
                } else {
                    sequence().let {
                        if (include)
                            it.isLessThanOrEqualTo(startId.sequence)
                        else
                            it.isLessThan(startId.sequence)
                    }
                    order(Order.REVERSE)
                }
            }.build()
        )
    }

    private suspend fun pullMoreByTimestamp(limit: Int): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (startTimestamp=$startTimestamp limit=$limit direction=${request.searchDirection})" }

        return context.cradleService.getMessagesBatchesSuspend(
            MessageFilterBuilder().apply {
                sessionAlias(streamName.sessionAlias)
                direction(streamName.direction)
                bookId(request.bookId)
                limit(limit)

                if (request.searchDirection == AFTER) {
                    timestampFrom().isGreaterThanOrEqualTo(startTimestamp)
                    order(Order.DIRECT)
                } else {
                    timestampTo().isLessThanOrEqualTo(startTimestamp)
                    order(Order.REVERSE)
                }
            }.build()
        )
    }

    private fun isLessThenStart(message: StoredMessage, startId: StoredMessageId?, include: Boolean = true): Boolean {
        return message.timestamp.isBeforeOrEqual(startTimestamp) &&
                startId?.let {
                    if (include) {
                        message.sequence <= it.sequence
                    } else {
                        message.sequence < it.sequence
                    }
                } ?: true
    }

    private fun isGreaterThenStart(message: StoredMessage, startId: StoredMessageId?,include: Boolean = true): Boolean {
        return message.timestamp.isAfterOrEqual(startTimestamp) &&
                startId?.let {
                    if (include) {
                        message.sequence >= it.sequence
                    } else {
                        message.sequence > it.sequence
                    }
                } ?: true
    }

    private fun getFirstIdInRange(batch: StoredMessageBatch, startId: StoredMessageId? = null, include: Boolean = true): Int? {
        val firstMessageInRange =
            if (request.searchDirection == AFTER) {
                batch.messages.indexOfFirst { isGreaterThenStart(it, startId, include) }
            } else {
                batch.messagesReverse.indexOfFirst { isLessThenStart(it, startId, include) }
            }

        return firstMessageInRange.takeIf { it != -1 }
    }

    suspend fun pullMoreMessageByTimestamp(limit: Int): List<MessageBatchWrapper> {
        return databaseRequestRetry(dbRetryDelay) {
            pullMoreByTimestamp(limit)
        }.mapNotNull { batch ->
            getFirstIdInRange(batch)?.let {
                MessageBatchWrapper(batch, it, request.searchDirection)
            }
        }
    }

    suspend fun pullMoreMessageById(startId: StoredMessageId, include: Boolean, limit: Int): List<MessageBatchWrapper> {
        return databaseRequestRetry(dbRetryDelay) {
            pullMoreById(startId, include, limit)
        }.mapNotNull { batch ->
            getFirstIdInRange(batch, startId, include)?.let {
                MessageBatchWrapper(batch, it, request.searchDirection)
            }
        }
    }
}