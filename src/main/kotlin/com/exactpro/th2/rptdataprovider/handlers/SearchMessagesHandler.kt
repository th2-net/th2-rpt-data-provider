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
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.RequestType
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.io.Writer
import java.lang.Integer.min
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

class SearchMessagesHandler(
    private val cradle: CradleService,
    private val messageCache: MessageCache,
    private val maxMessagesLimit: Int,
    private val messageSearchPipelineBuffer: Int,
    private val dbRetryDelay: Long
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
            messageBatch.find { it.timestamp.isAfter(timestamp) } ?: messageBatch.lastOrNull()
        else
            messageBatch.findLast { it.timestamp.isBefore(timestamp) } ?: messageBatch.firstOrNull()

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
        timestamp: Instant
    ): MutableMap<Pair<String, Direction>, StoredMessageId?> {

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

    @ExperimentalCoroutinesApi
    @FlowPreview
    private suspend fun getMessageStream(
        streamMessageIndexMap: MutableMap<Pair<String, Direction>, StoredMessageId?>,
        timelineDirection: TimeRelation,
        initLimit: Int,
        messageId: String?,
        startTimestamp: Instant,
        timestampFrom: Instant?,
        timestampTo: Instant?,
        requestType: RequestType
    ): Flow<StoredMessage> {
        return coroutineScope {
            flow {
                var limit = initLimit
                do {
                    val data = pullMoreMerged(
                        streamMessageIndexMap,
                        timelineDirection,
                        limit,
                        startTimestamp,
                        requestType
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
                            timestampTo == null || timestamp.isBeforeOrEqual(timestampTo)
                        } else {
                            timestampFrom == null || timestamp.isAfterOrEqual(timestampFrom)

                        }
                    }
                }
        }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun searchMessages(request: MessageSearchRequest): List<Any> {
        return coroutineScope {
            val bufferSize = chooseBufferSize(request)
            // Small optimization
            val messagesFromAttachedId =
                request.attachedEventId?.let { cradle.getMessageIdsSuspend(StoredTestEventId(it)) }

            val startTimestamp = chooseStartTimestamp(
                request.messageId, request.timelineDirection,
                request.timestampFrom, request.timestampTo
            )

            val streamMessageIndexMap = initStreamMessageIdMap(
                request.timelineDirection, request.stream,
                startTimestamp
            )
            getMessageStream(
                streamMessageIndexMap, request.timelineDirection, request.limit,
                request.messageId, startTimestamp, request.timestampFrom, request.timestampTo, RequestType.REST
            )
                .map {
                    async {
                        if ((request.attachedEventId ?: request.messageType) != null || !request.idsOnly) {
                            @Suppress("USELESS_CAST")
                            Pair(
                                it,
                                isMessageMatched(
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


    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun searchMessagesSse(
        request: SseMessageSearchRequest,
        jacksonMapper: ObjectMapper,
        keepAlive: suspend (Writer, lastId: LastScannedObjectInfo, counter: AtomicLong) -> Unit,
        writer: Writer
    ) {
        withContext(coroutineContext) {
            val messageId = request.resumeFromId

            val startTimestamp = chooseStartTimestamp(
                messageId, request.searchDirection,
                request.startTimestamp, request.startTimestamp
            )
            val streamMessageIndexMap = initStreamMessageIdMap(
                request.searchDirection, request.stream,
                startTimestamp
            )
            val startMessageCountLimit = 25
            val lastScannedObject = LastScannedObjectInfo()
            val lastEventId = AtomicLong(0)
            getMessageStream(
                streamMessageIndexMap, request.searchDirection, request.resultCountLimit ?: startMessageCountLimit,
                messageId, startTimestamp, request.endTimestamp, request.endTimestamp, RequestType.SSE
            ).map {
                async {
                    @Suppress("USELESS_CAST")
                    Pair(it, request.filterPredicate.apply(messageCache.getOrPut(it)))
                }.also { coroutineContext.ensureActive() }
            }
                .buffer(messageSearchPipelineBuffer)
                .map { it.await() }
                .onEach {
                    lastScannedObject.apply { id = it.first.id.toString(); timestamp = it.first.timestamp.toEpochMilli() }
                }
                .filter { it.second }
                .map { it.first }
                .let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive.invoke(writer,lastScannedObject, lastEventId)
                    }
                }
                .onCompletion {
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()
                    writer.eventWrite(
                        SseEvent.build(jacksonMapper, messageCache.getOrPut(it), lastEventId)
                    )
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

    private fun dropUntilInRangeInOppositeDirection(
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
        startTimestamp: Instant,
        requestType: RequestType
    ): List<StoredMessage> {
        logger.debug { "pulling more messages (streams=${streamMessageIndexMap.keys} direction=$timelineDirection perStreamLimit=$perStreamLimit)" }
        return streamMessageIndexMap.keys
            .flatMap { stream ->
                if (requestType == RequestType.SSE) {
                    databaseRequestRetry(dbRetryDelay) {
                        pullMore(streamMessageIndexMap[stream], perStreamLimit, timelineDirection)
                    }
                } else {
                    pullMore(streamMessageIndexMap[stream], perStreamLimit, timelineDirection)
                }.toList()
                    .let {
                        val streamIsEmpty = it.size < perStreamLimit
                        val filteredIdsList = dropUntilInRangeInOppositeDirection(startTimestamp, it, timelineDirection)
                        streamMessageIndexMap[stream] = if (streamIsEmpty) null else filteredIdsList.lastOrNull()?.id
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
