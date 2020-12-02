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
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.asyncClose
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.sse.EventType
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.eventWrite
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.lang.Integer.min
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.coroutines.coroutineContext
import kotlin.reflect.KSuspendFunction1

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

    private suspend fun isMessageMatched(
        messageType: List<String>?,
        messagesFromAttachedId: Collection<StoredMessageId>?,
        message: Message
    ): Boolean {
        return (messageType == null || messageType.any { item ->
            message.messageType.toLowerCase().contains(item.toLowerCase())
        }) && (messagesFromAttachedId == null || messagesFromAttachedId.let {
            val messageId = StoredMessageId.fromString(message.messageId)
            it.contains(messageId)
        })
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

    private suspend fun chooseStartTimestamp(
        messageId: String?,
        timelineDirection: TimeRelation,
        timestampFrom: Instant?,
        timestampTo: Instant?
    ): Instant {
        return messageId?.let {
            cradle.getMessageSuspend(StoredMessageId.fromString(it))?.timestamp
        } ?: (
                if (timelineDirection == TimeRelation.AFTER) {
                    timestampFrom
                } else {
                    timestampTo
                }) ?: Instant.now()
    }

    private suspend fun getNearestMessage(
        messageBatch: Collection<StoredMessage>,
        timelineDirection: TimeRelation,
        timestamp: Instant
    ): StoredMessage? {
        if (messageBatch.isEmpty()) return null
        return if (timelineDirection == TimeRelation.AFTER)
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
            cradle.getFirstMessageIdSuspend(timestamp, stream, direction, timeRelation)
                ?.let { return it }
        }
        return null
    }

    private suspend fun getFirstMessageIdDifferentDays(
        startTimestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation,
        daysInterval: Int = 2
    ): StoredMessageId? {
        var daysChecking = daysInterval
        var isCurrentDay = true
        var timestamp = startTimestamp
        var messageId: StoredMessageId? = null
        while (messageId == null && daysChecking >= 0) {
            messageId =
                if (isCurrentDay) {
                    getFirstMessageCurrentDay(timestamp, stream, direction)
                } else {
                    cradle.getFirstMessageIdSuspend(timestamp, stream, direction, timelineDirection)
                }
            daysChecking -= 1
            isCurrentDay = false
            timestamp = nextDay(timestamp, timelineDirection)
        }
        return messageId
    }

    private suspend fun initStreamMessageIdMap(
        timelineDirection: TimeRelation,
        streamList: List<String>?,
        messageId: String?,
        timestampFrom: Instant?,
        timestampTo: Instant?
    ): MutableMap<Pair<String, Direction>, StoredMessageId?> {
        val timestamp = chooseStartTimestamp(messageId, timelineDirection, timestampFrom, timestampTo)

        return mutableMapOf<Pair<String, Direction>, StoredMessageId?>().apply {
            for (stream in streamList ?: emptyList()) {
                for (direction in Direction.values()) {
                    val storedMessageId =
                        getFirstMessageIdDifferentDays(timestamp, stream, direction, timelineDirection)
                    if (storedMessageId != null) {
                        val messageBatch = cradle.getMessageBatchSuspend(storedMessageId)
                        put(
                            Pair(stream, direction),
                            getNearestMessage(messageBatch, timelineDirection, timestamp)?.id
                        )
                    } else {
                        put(Pair(stream, direction), null)
                    }
                }
            }
        }
    }

    private suspend fun getMessageStream(
        streamMessageIndexMap: MutableMap<Pair<String, Direction>, StoredMessageId?>,
        timelineDirection: TimeRelation,
        initLimit: Int,
        messageId: String?,
        timestampFrom: Instant?,
        timestampTo: Instant?
    ): Flow<StoredMessage> {
        return coroutineScope {
            flow {
                var limit = initLimit
                do {
                    val data = pullMoreMerged(
                        streamMessageIndexMap,
                        timelineDirection,
                        limit
                    )
                    for (item in data) {
                        emit(item)
                    }
                    limit = min(maxMessagesLimit, limit * 2)
                } while (data.size >= limit)
            }
                .filterNot { it.id.toString() == messageId }
                .distinctUntilChanged { old, new -> old.id == new.id }
                .takeWhile {
                    it.timestamp.let { timestamp ->
                        if (timelineDirection == TimeRelation.AFTER) {
                            timestampTo == null
                                    || timestamp.isBefore(timestampTo) || timestamp == timestampTo
                        } else {
                            timestampFrom == null
                                    || timestamp.isAfter(timestampFrom) || timestamp == timestampFrom
                        }
                    }
                }
        }
    }

    suspend fun searchMessages(request: MessageSearchRequest): List<Any> {
        return coroutineScope {
            val bufferSize = chooseBufferSize(request)
            // Small optimization
            val messagesFromAttachedId =
                request.attachedEventId?.let { cradle.getMessageIdsSuspend(StoredTestEventId(it)) }

            val streamMessageIndexMap = initStreamMessageIdMap(
                request.timelineDirection, request.stream,
                request.messageId, request.timestampFrom, request.timestampTo
            )
            getMessageStream(
                streamMessageIndexMap, request.timelineDirection, request.limit,
                request.messageId, request.timestampFrom, request.timestampTo
            )
                .map {
                    async {
                        if ((request.attachedEventId ?: request.messageType) != null || !request.idsOnly) {
                            @Suppress("USELESS_CAST")
                            Pair(
                                it, isMessageMatched(
                                    request.messageType,
                                    messagesFromAttachedId,
                                    messageCache.getOrPut(it)
                                )
                            )
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

    private suspend fun getTimePair(
        searchDirection: TimeRelation,
        startTimestamp: Instant,
        timeLimit: Long
    ): Pair<Instant, Instant> {
        return if (searchDirection == TimeRelation.AFTER) {
            Pair(startTimestamp, startTimestamp.plusMillis(timeLimit))
        } else {
            Pair(startTimestamp.minusMillis(timeLimit), startTimestamp)
        }
    }

    private suspend fun isSseMessageMatched(
        type: List<String>?,
        messagesFromAttachedId: Collection<Collection<StoredMessageId>>?,
        message: Message,
        negativeTypeFilter: Boolean
    ): Boolean {
        return (type == null || negativeTypeFilter.xor(type.any { item ->
            message.messageType.toLowerCase().contains(item.toLowerCase())
        })) && (messagesFromAttachedId == null || messagesFromAttachedId.let { messagesFromEventId ->
            val messageId = StoredMessageId.fromString(message.messageId)
            messagesFromEventId.any { it.contains(messageId) }
        })
    }

    @ExperimentalCoroutinesApi
    suspend fun searchMessagesSse(
        request: SseMessageSearchRequest,
        call: ApplicationCall,
        jacksonMapper: ObjectMapper,
        exceptionConverter: (Exception) -> String
    ) {
        withContext(coroutineContext) {
            call.respondTextWriter(contentType = ContentType.Text.EventStream) {
                val messageId = null
                val timePair = getTimePair(request.searchDirection, request.startTimestamp, request.timeLimit)
                val streamMessageIndexMap = initStreamMessageIdMap(
                    request.searchDirection, request.stream,
                    messageId, timePair.first, timePair.second
                )
                val messagesFromAttachedId =
                    request.attachedEventIds?.let { ids ->
                        ids.map { cradle.getMessageIdsSuspend(StoredTestEventId(it)) }
                    }
                getMessageStream(
                    streamMessageIndexMap, request.searchDirection, request.resultCountLimit,
                    messageId, timePair.first, timePair.second
                ).map {
                    async {
                        @Suppress("USELESS_CAST")
                        Pair(
                            it, isSseMessageMatched(
                                request.type, messagesFromAttachedId,
                                messageCache.getOrPut(it), request.negativeTypeFilter
                            )
                        )
                    }.also { coroutineContext.ensureActive() }
                }
                    .buffer(messageSearchPipelineBuffer)
                    .map { it.await() }
                    .filter { it.second }
                    .map { it.first }
                    .take(request.resultCountLimit)
                    .catch { eventWrite(SseEvent(exceptionConverter.invoke(it as Exception), event = EventType.ERROR)) }
                    .onCompletion {
                        eventWrite(SseEvent(event = EventType.CLOSE))
                        asyncClose()
                        it?.let { throwable -> throw throwable }
                    }
                    .collect {
                        coroutineContext.ensureActive()
                        eventWrite(
                            SseEvent(
                                jacksonMapper.asStringSuspend(messageCache.getOrPut(it)),
                                EventType.MESSAGE,
                                it.id.toString()
                            )
                        )
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

    private suspend fun pullMoreMerged(
        streamMessageIndexMap: MutableMap<Pair<String, Direction>, StoredMessageId?>,
        timelineDirection: TimeRelation,
        perStreamLimit: Int
    ): List<StoredMessage> {
        logger.debug { "pulling more messages (streams=${streamMessageIndexMap.keys} direction=$timelineDirection perStreamLimit=$perStreamLimit)" }
        return streamMessageIndexMap.keys
            .flatMap { stream ->
                pullMore(
                    streamMessageIndexMap[stream],
                    perStreamLimit,
                    timelineDirection
                ).let {
                    streamMessageIndexMap[stream] = if (it.isNotEmpty()) it.last().id else null
                    it
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
