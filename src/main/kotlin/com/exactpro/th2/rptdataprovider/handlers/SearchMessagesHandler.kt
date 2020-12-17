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
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import kotlinx.coroutines.async
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

    private suspend fun getNearestMessage(
        messageBatch: Collection<StoredMessage>,
        request: MessageSearchRequest,
        timestamp: Instant
    ): StoredMessage? {
        if (messageBatch.isEmpty()) return null
        return if (request.timelineDirection == TimeRelation.AFTER)
            messageBatch.find { it.timestamp.isAfter(timestamp) } ?: messageBatch.last()
        else
            messageBatch.findLast { it.timestamp.isBefore(timestamp) } ?: messageBatch.first()

    }

    private suspend fun getFirstMessageCurrentDay(
        timestamp: Instant,
        stream: String,
        direction: Direction
    ): StoredMessageId? {
        for (timeRelation in listOf(TimeRelation.BEFORE, TimeRelation.AFTER)) {
            cradle.getFirstMessageIdSuspend(
                timestamp,
                stream,
                direction,
                timeRelation
            )?.let { return it }
        }
        return null
    }

    private suspend fun getFirstMessageIdDifferentDays(
        startTimestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation
    ): StoredMessageId? {
        var daysChecking = 2
        var isCurrentDay = true
        var timestamp = startTimestamp
        var messageId: StoredMessageId? = null
        while (messageId == null && daysChecking >= 0) {
            messageId =
                if (isCurrentDay) {
                    getFirstMessageCurrentDay(timestamp, stream, direction)
                } else {
                    cradle.getFirstMessageIdSuspend(
                        timestamp,
                        stream,
                        direction,
                        timelineDirection
                    )
                }
            daysChecking -= 1
            isCurrentDay = false
            timestamp = nextDay(timestamp, timelineDirection)
        }
        return messageId
    }

    private suspend fun initStreamMessageIdMap(startTimestamp: Instant, request: MessageSearchRequest):
            MutableMap<Pair<String, Direction>, StoredMessageId?> {

        return mutableMapOf<Pair<String, Direction>, StoredMessageId?>().apply {
            for (stream in request.stream ?: emptyList()) {
                for (direction in Direction.values()) {
                    val storedMessageId =
                        getFirstMessageIdDifferentDays(startTimestamp, stream, direction, request.timelineDirection)
                    if (storedMessageId != null) {
                        val messageBatch = cradle.getMessageBatchSuspend(storedMessageId)

                        put(
                            Pair(stream, direction),
                            getNearestMessage(messageBatch, request, startTimestamp)?.id
                        )
                    } else {
                        put(Pair(stream, direction), null)
                    }
                }
            }
        }
    }

    suspend fun searchMessages(request: MessageSearchRequest): List<Any> {
        return coroutineScope {
            val bufferSize = chooseBufferSize(request)
            val startTimestamp = chooseStartTimestamp(request)
            val streamMessageIndexMap = initStreamMessageIdMap(startTimestamp, request)
            flow {
                var limit = request.limit
                do {
                    val data = pullMoreMerged(
                        streamMessageIndexMap,
                        request.timelineDirection,
                        limit,
                        startTimestamp
                    )
                    for (item in data) {
                        emit(item)
                    }
                    limit = min(maxMessagesLimit, limit * 2)
                } while (data.size >= limit)
            }
                .filterNot { it.id.toString() == request.messageId }
                .distinctUntilChanged { old, new -> old.id == new.id }
                .takeWhile {
                    it.timestamp.let { timestamp ->
                        if (request.timelineDirection == TimeRelation.AFTER) {
                            request.timestampTo == null || timestamp.isBeforeOrEqual(request.timestampTo)
                        } else {
                            request.timestampFrom == null || timestamp.isAfterOrEqual(request.timestampFrom)
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

    private fun dropIfTimestamp(
        startTimestamp: Instant,
        storedMessages: List<StoredMessage>,
        timelineDirection: TimeRelation
    ): List<StoredMessage> {
        return if (timelineDirection == TimeRelation.AFTER) {
            storedMessages.dropWhile { it.timestamp.isBefore(startTimestamp) }
        } else {
            storedMessages.dropWhile { it.timestamp.isAfter(startTimestamp) }
        }
    }

    private suspend fun pullMoreMerged(
        streamMessageIndexMap: MutableMap<Pair<String, Direction>, StoredMessageId?>,
        timelineDirection: TimeRelation,
        perStreamLimit: Int,
        startTimestamp: Instant
    ): List<StoredMessage> {
        logger.debug { "pulling more messages (streams=${streamMessageIndexMap.keys} direction=$timelineDirection perStreamLimit=$perStreamLimit)" }
        return streamMessageIndexMap.keys
            .flatMap { stream ->
                pullMore(
                    streamMessageIndexMap[stream],
                    perStreamLimit,
                    timelineDirection
                ).let {
                    val filteredIdsList = dropIfTimestamp(startTimestamp, it, timelineDirection)
                    streamMessageIndexMap[stream] =
                        if (filteredIdsList.isNotEmpty()) filteredIdsList.last().id else null
                    filteredIdsList
                }
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

