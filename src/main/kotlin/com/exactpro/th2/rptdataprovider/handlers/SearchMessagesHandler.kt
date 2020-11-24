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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.lang.Integer.min
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset

class SearchMessagesHandler(
    private val cradle: CradleService,
    private val messageCache: MessageCache,
    private val messageProducer: MessageProducer,
    private val maxMessagesLimit: Int,
    private val messageSearchPipelineBuffer: Int
) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private suspend fun isMessageMatched(request: MessageSearchRequest, message: Message): Boolean {
        return (request.messageType == null || request.messageType.any { item ->
            message.messageType.toLowerCase().contains(item.toLowerCase())
        }) && (request.attachedEventId == null
                || cradle.getMessageIdsSuspend(StoredTestEventId(request.attachedEventId))
                .contains(StoredMessageId.fromString(message.messageId)))
    }

    private fun chooseBufferSize(request: MessageSearchRequest): Int {
        return if ((request.attachedEventId ?: request.messageType) == null)
            request.limit
        else
            messageSearchPipelineBuffer
    }

    private suspend fun chooseStartTimestamp(request: MessageSearchRequest): Instant {
        return request.messageId?.let {
            cradle.getMessageSuspend(StoredMessageId.fromString(it))?.timestamp
        } ?: (
                if (request.timelineDirection == TimeRelation.AFTER) {
                    request.timestampFrom
                } else {
                    request.timestampTo
                }) ?: Instant.now()
    }

    suspend fun searchMessages(request: MessageSearchRequest): List<Any> {
        return coroutineScope {
            val bufferSize = chooseBufferSize(request)
            flow {
                var timestamp = chooseStartTimestamp(request)
                var limit = request.limit
                do {
                    val data = pullMoreMerged(
                        timestamp,
                        request.stream ?: emptyList(),
                        request.timelineDirection,
                        limit
                    )
                    for (item in data) {
                        emit(item)
                    }
                    if (data.isNotEmpty())
                        timestamp = data.last().timestamp

                    limit = min(maxMessagesLimit, limit * 2)
                } while (data.isNotEmpty())
            }
                    .filterNot { it.id.toString() == request.messageId }
                    .distinctUntilChanged { old, new -> old.id == new.id }
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
                    .map {
                        async {
                            if ((request.attachedEventId ?: request.messageType) != null || !request.idsOnly) {
                                @Suppress("USELESS_CAST")
                                Pair(it, isMessageMatched(request, messageCache.getOrPut(it)))
                            } else {
                                Pair(it, true)
                            }
                        }
                    }
                    .buffer(bufferSize)
                    .map { it.await() }
                    .filter { it.second }
                    .map {
                        if (request.idsOnly) {
                            it.first.id.toString()
                        } else {
                            messageCache.getOrPut(it.first)
                        }
                    }
                    .take(request.limit)
                    .toList()
        }
    }

    private fun nextDay(timestamp: Instant, timelineDirection: TimeRelation): Instant {
        val utcTimestamp = timestamp.atOffset(ZoneOffset.UTC)
        return if (timelineDirection == TimeRelation.AFTER) {
            utcTimestamp.plusDays(1)
                    .with(LocalTime.of(0, 0, 0, 0))
        } else {
            utcTimestamp.with(LocalTime.of(0, 0, 0, 0))
                    .minusNanos(1)
        }.toInstant()
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
                            it.streamName().isEqualTo(startId.streamName)
                                    .direction().isEqualTo(startId.direction)
                                    .index().isGreaterThanOrEqualTo(startId.index)
                                    .limit(limit)
                        } else {
                            it.streamName().isEqualTo(startId.streamName)
                                    .direction().isEqualTo(startId.direction)
                                    .index().isLessThanOrEqualTo(startId.index)
                                    .limit(limit)
                        }
                    }.build()
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

    private suspend fun pullFullLimit(
        stream: String,
        startTimestamp: Instant,
        timelineDirection: TimeRelation,
        perStreamLimit: Int
    ): List<StoredMessage> {
        return mutableListOf<StoredMessage>().apply {
            var timestamp = startTimestamp
            var daysChecking = 2
            do {
                for (direction in Direction.values()) {
                    addAll(
                        pullMore(
                            cradle.getFirstMessageIdSuspend(
                                timestamp,
                                stream,
                                direction,
                                timelineDirection
                            ),
                            perStreamLimit,
                            timelineDirection
                        )
                    )
                }
                daysChecking -= 1
                timestamp = nextDay(timestamp, timelineDirection)
            } while (this.size <= perStreamLimit && daysChecking > 0)
        }
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
                .flatMap { stream ->
                    pullFullLimit(stream, startTimestamp, timelineDirection, perStreamLimit)
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

