/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.cache.MessageCache
import com.exactpro.th2.reportdataprovider.entities.requests.MessageSearchRequest
import com.exactpro.th2.reportdataprovider.entities.responses.Message
import com.exactpro.th2.reportdataprovider.producers.MessageProducer
import com.exactpro.th2.reportdataprovider.services.CradleService
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Instant

class SearchMessagesHandler(
    private val cradle: CradleService,
    private val messageCache: MessageCache,
    private val messageProducer: MessageProducer,
    private val timeout: Long
) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    suspend fun isMessageFiltered(request: MessageSearchRequest, message: Message): Boolean {
        return (request.messageType == null || request.messageType.any { item ->
            message.messageType.toLowerCase().contains(item.toLowerCase())
        }) && (request.attachedEventId == null
                || cradle.getMessageIdsSuspend(StoredTestEventId(request.attachedEventId))
            .contains(StoredMessageId.fromString(message.messageId)))
    }


    suspend fun searchMessages(request: MessageSearchRequest): List<Any> {
        return withTimeout(timeout) {

            flow {
                do {
                    val data = pullMoreMerged(
                        request.messageId?.let {
                            cradle.getMessageSuspend(StoredMessageId.fromString(it))?.timestamp
                        } ?: (
                                if (request.timelineDirection == TimeRelation.AFTER) {
                                    request.timestampFrom
                                } else {
                                    request.timestampTo
                                }

                                ) ?: Instant.now(),

                        request.stream ?: emptyList(),
                        request.timelineDirection,
                        request.limit
                    )

                    for (item in data) {
                        emit(item)
                    }
                } while (data.isNotEmpty())
            }
                .takeWhile {
                    it.timestamp.let { timestamp ->
                        if (request.timelineDirection == TimeRelation.AFTER) {
                            request.timestampTo == null
                                    || timestamp.isBefore(request.timestampTo) || timestamp == request.timestampTo
                        } else {
                            request.timestampFrom == null
                                    || timestamp.isAfter(request.timestampFrom) || timestamp == request.timestampFrom
                        }
                    }
                }
                .take(request.limit)
                .toList()
                .distinctBy { it.id }
                .filterNot { it.id.toString() == request.messageId }
                .map {
                    async {
                        if ((request.attachedEventId ?: request.messageType) != null || !request.idsOnly) {
                            @Suppress("USELESS_CAST")
                            Pair(it, isMessageFiltered(request, messageCache.getOrPut(it)))
                        } else {
                            Pair(it, true)
                        }
                    }
                }
                .map { it.await() }
                .filter { it.second }
                .map {
                    if (request.idsOnly) {
                        it.first.id.toString()
                    } else {
                        messageCache.getOrPut(it.first)
                    }
                }
        }
    }

    private suspend fun pullMore(
        startId: StoredMessageId?,
        limit: Int,
        timelineDirection: TimeRelation
    ): List<StoredMessage> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=$timelineDirection)" }

        if (startId == null) return emptyList()

        return cradle.getMessagesSuspend(
            StoredMessageFilterBuilder()
                .let {
                    if (timelineDirection == TimeRelation.AFTER) {
                        it.next(startId, limit)
                    } else {
                        it.previous(startId, limit)
                    }
                }
                .build()
        )

            .let { list ->
                if (timelineDirection == TimeRelation.AFTER) {
                    list.sortedBy { it.timestamp }
                } else {
                    list.sortedByDescending { it.timestamp }
                }
            }
            .toList()
    }

    private suspend fun pullMoreMerged(
        startTimestamp: Instant,
        streams: List<String>,
        timelineDirection: TimeRelation,
        perStreamLimit: Int
    ): List<StoredMessage> {
        logger.debug { "pulling more messages (timestamp=$startTimestamp streams=$streams direction=$timelineDirection perStreamLimit=$perStreamLimit)" }

        return streams
            .distinct()
            .flatMap { listOf(it to Direction.FIRST, it to Direction.SECOND) }
            .flatMap { pair ->
                pullMore(
                    cradle.getFirstMessageIdSuspend(
                        startTimestamp,
                        pair.first,
                        pair.second,
                        timelineDirection
                    ),
                    perStreamLimit,
                    timelineDirection
                )
            }
            .let { list ->
                if (timelineDirection == TimeRelation.AFTER) {
                    list.sortedBy { it.timestamp }
                } else {
                    list.sortedByDescending { it.timestamp }
                }
            }
    }


}

