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

import com.exactpro.cradle.TimeRelation
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

class StreamGenerator(
    private val context: Context,
    private val coroutineScope: CoroutineScope
) {

    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val dbRetryDelay = context.configuration.dbRetryDelay.value.toLong()

    private suspend fun pullMore(
        startId: StoredMessageId?,
        timelineDirection: TimeRelation,
        limit: Int,
        isFirstPull: Boolean
    ): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=$timelineDirection)" }

        if (startId == null) return emptyList()

        return context.cradleService.getMessagesBatchesSuspend(
            StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(startId.streamName)
                direction().isEqualTo(startId.direction)
                limit(limit)

                if (timelineDirection == TimeRelation.AFTER) {
                    index().let {
                        if (isFirstPull)
                            it.isGreaterThanOrEqualTo(startId.index)
                        else
                            it.isGreaterThan(startId.index)
                    }
                } else {
                    index().let {
                        if (isFirstPull)
                            it.isLessThanOrEqualTo(startId.index)
                        else
                            it.isLessThan(startId.index)
                    }
                }
            }.build()
        )
    }


    suspend fun pullMoreWrapped(
        startId: StoredMessageId?,
        limit: Int,
        request: SseMessageSearchRequest,
        isFirstPull: Boolean
    ): List<MessageWrapper> {
        return coroutineScope {
            databaseRequestRetry(dbRetryDelay) {
                pullMore(startId, request.searchDirection, limit, isFirstPull)
            }.map { batch ->
                async {
                    val parsedFuture = coroutineScope.async {
                        context.messageProducer.fromRawMessage(batch, request)
                    }
                    batch.messages.map { message ->
                        MessageWrapper.build(message, parsedFuture)
                    }
                }
            }.awaitAll().flatten()
        }
    }

    suspend fun pullMoreSorted(
        startId: StoredMessageId?,
        limit: Int,
        request: SseMessageSearchRequest,
        isFirstPull: Boolean
    ): List<MessageWrapper> {
        return coroutineScope {
            pullMoreWrapped(startId, limit, request, isFirstPull).let { list ->
                if (request.searchDirection == TimeRelation.AFTER) {
                    list.sortedBy { it.message.timestamp }
                } else {
                    list.sortedByDescending { it.message.timestamp }
                }
            }
        }
    }
}