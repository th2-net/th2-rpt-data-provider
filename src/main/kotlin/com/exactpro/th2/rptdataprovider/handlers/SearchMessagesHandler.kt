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
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import com.fasterxml.jackson.databind.ObjectMapper
import io.prometheus.client.Counter
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
import kotlin.system.measureTimeMillis

class SearchMessagesHandler(
    private val cradle: CradleService,
    private val messageProducer: MessageProducer,
    private val messageCache: MessageCache,
    private val maxMessagesLimit: Int,
    private val messageSearchPipelineBuffer: Int,
    private val dbRetryDelay: Long,
    private val sseSearchDelay: Long
) {
    companion object {
        private val logger = KotlinLogging.logger { }

        private val processedMessageCount = Counter.build(
            "processed_message_count", "Count of processed Message"
        ).register()
    }

    data class StreamInfo(val stream: Pair<String, Direction>, val keepOpen: Boolean) {
        var lastElement: StoredMessageId? = null
            private set
        var isFirstPull: Boolean = true
            private set

        constructor(stream: Pair<String, Direction>, startId: StoredMessageId?, keepOpen: Boolean) : this(
            stream = stream,
            keepOpen = keepOpen
        ) {
            lastElement = startId
        }

        fun update(
            size: Int,
            perStreamLimit: Int,
            timelineDirection: TimeRelation,
            filteredIdsList: List<MessageWrapper>
        ) {
            val streamIsEmpty = size < perStreamLimit
            lastElement = changeStreamMessageIndex(streamIsEmpty, filteredIdsList, timelineDirection)
            isFirstPull = false
        }


        private fun changeStreamMessageIndex(
            streamIsEmpty: Boolean,
            filteredIdsList: List<MessageWrapper>,
            timelineDirection: TimeRelation
        ): StoredMessageId? {
            return if (timelineDirection == AFTER) {
                filteredIdsList.lastOrNull()?.message?.id
                    ?: if (keepOpen) lastElement else null
            } else {
                if (!streamIsEmpty) filteredIdsList.firstOrNull()?.message?.id else null
            }
        }
    }

    private fun nextDay(timestamp: Instant, timelineDirection: TimeRelation): Instant {
        val utcTimestamp = timestamp.atOffset(ZoneOffset.UTC)
        return if (timelineDirection == AFTER) {
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
                if (timelineDirection == AFTER) {
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
        return if (timelineDirection == AFTER)
            messageBatch.find { it.timestamp.isAfter(timestamp) } ?: messageBatch.lastOrNull()
        else
            messageBatch.findLast { it.timestamp.isBefore(timestamp) } ?: messageBatch.firstOrNull()

    }

    private suspend fun getFirstMessageCurrentDay(
        timestamp: Instant,
        stream: String,
        direction: Direction
    ): StoredMessageId? {
        for (timeRelation in listOf(BEFORE, AFTER)) {
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


    private suspend fun initStreamsInfo(
        timelineDirection: TimeRelation,
        streamList: List<String>?,
        timestamp: Instant,
        keepOpen: Boolean = false
    ): List<StreamInfo> {

        return mutableListOf<StreamInfo>().apply {
            for (stream in streamList ?: emptyList()) {
                for (direction in Direction.values()) {
                    val storedMessageId =
                        getFirstMessageIdDifferentDays(timestamp, stream, direction, timelineDirection)

                    if (storedMessageId != null) {
                        val messageBatch = cradle.getMessageBatchSuspend(storedMessageId)
                        add(
                            StreamInfo(
                                Pair(stream, direction),
                                getNearestMessage(messageBatch, timelineDirection, timestamp)?.id,
                                keepOpen
                            )
                        )
                    } else {
                        add(StreamInfo(Pair(stream, direction), null, keepOpen))
                    }
                }
            }
        }
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    private suspend fun getMessageStream(
        request: SseMessageSearchRequest,
        streamsInfo: List<StreamInfo>,
        initLimit: Int,
        messageId: StoredMessageId?,
        startTimestamp: Instant
    ): Flow<MessageWrapper> {

        return coroutineScope {
            flow {
                var limit = min(maxMessagesLimit, initLimit)
                var isSearchInFuture = false
                do {
                    if (isSearchInFuture)
                        delay(sseSearchDelay * 1000)

                    val data = pullMoreMerged(streamsInfo, request.searchDirection, limit, startTimestamp, messageId)

                    for (item in data) {
                        emit(item)
                    }

                    val anyStreamIsNotEmpty = streamsInfo.any { it.lastElement != null }

                    val canGetData = isSearchInFuture || anyStreamIsNotEmpty ||
                            if (request.keepOpen && request.searchDirection == AFTER) {
                                isSearchInFuture = true
                                true
                            } else {
                                false
                            }

                    limit = min(maxMessagesLimit, limit * 2)
                } while (canGetData)
            }
                .takeWhile {
                    it.message.timestamp.let { timestamp ->
                        if (request.searchDirection == AFTER) {
                            request.endTimestamp == null || timestamp.isBeforeOrEqual(request.endTimestamp)
                        } else {
                            request.endTimestamp == null || timestamp.isAfterOrEqual(request.endTimestamp)
                        }
                    }
                }
        }
    }


    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun searchMessagesSse(
        request: SseMessageSearchRequest,
        jacksonMapper: ObjectMapper,
        keepAlive: suspend (Writer, lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) -> Unit,
        writer: Writer
    ) {
        withContext(coroutineContext) {
            val startMessageCountLimit = 25
            val lastScannedObject = LastScannedObjectInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)
            val messageIdStored = request.resumeFromId?.let { StoredMessageId.fromString(it) }
            flow {
                val startTimestamp = chooseStartTimestamp(
                    request.resumeFromId, request.searchDirection,
                    request.startTimestamp, request.startTimestamp
                )

                val streamsInfo = initStreamsInfo(
                    request.searchDirection, request.stream,
                    startTimestamp, request.keepOpen
                )
                getMessageStream(
                    request, streamsInfo, request.resultCountLimit ?: startMessageCountLimit,
                    messageIdStored, startTimestamp
                ).collect { emit(it) }
            }.map {
                async {
                    val parsedMessage = it.getParsedMessage()
                    messageCache.put(parsedMessage.messageId, parsedMessage)
                    Pair(parsedMessage, request.filterPredicate.apply(parsedMessage))
                }.also { coroutineContext.ensureActive() }
            }
                .buffer(messageSearchPipelineBuffer)
                .map { it.await() }
                .onEach { (message, _) ->
                    lastScannedObject.update(message, scanCnt)
                    processedMessageCount.inc()
                }
                .filter { it.second }
                .map { it.first }
                .let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive.invoke(writer, lastScannedObject, lastEventId)
                    }
                }
                .onCompletion {
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()
                    writer.eventWrite(
                        SseEvent.build(jacksonMapper, it, lastEventId)
                    )
                }
        }
    }


    private suspend fun pullMore(
        startId: StoredMessageId?,
        limit: Int,
        timelineDirection: TimeRelation,
        isFirstPull: Boolean
    ): Iterable<StoredMessageBatch> {

        logger.debug { "pulling more messages (id=$startId limit=$limit direction=$timelineDirection)" }

        if (startId == null) return emptyList()

        return cradle.getMessagesBatchesSuspend(
            StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(startId.streamName)
                direction().isEqualTo(startId.direction)
                limit(limit)

                if (timelineDirection == AFTER) {
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


    private suspend fun pullMoreWrapped(
        startId: StoredMessageId?,
        limit: Int,
        timelineDirection: TimeRelation,
        isFirstPull: Boolean
    ): List<MessageWrapper> {
        return coroutineScope {

            databaseRequestRetry(dbRetryDelay) {
                pullMore(startId, limit, timelineDirection, isFirstPull)
            }.map { batch ->
                async {
                    val parsedFuture = CoroutineScope(Dispatchers.Default).async {
                        messageProducer.fromRawMessage(batch)
                    }
                    batch.messages.map { message ->
                        MessageWrapper.build(message, parsedFuture)
                    }
                }
            }.awaitAll().flatten()
        }
    }


    private fun isInFuture(wrapper: MessageWrapper, startTimestamp: Instant, streamInfo: StreamInfo): Boolean {
        return wrapper.message.timestamp.isAfterOrEqual(startTimestamp) &&
                streamInfo.lastElement?.let {
                    if (streamInfo.isFirstPull) {
                        wrapper.message.index >= it.index
                    } else {
                        wrapper.message.index > it.index
                    }
                } ?: true
    }

    private fun isInPast(wrapper: MessageWrapper, startTimestamp: Instant, streamInfo: StreamInfo): Boolean {
        return wrapper.message.timestamp.isBeforeOrEqual(startTimestamp) &&
                streamInfo.lastElement?.let {
                    if (streamInfo.isFirstPull) {
                        wrapper.message.index <= it.index
                    } else {
                        wrapper.message.index < it.index
                    }
                } ?: true
    }

    private fun dropUntilInRangeInOppositeDirection(
        startTimestamp: Instant,
        streamInfo: StreamInfo,
        storedMessages: List<MessageWrapper>,
        timelineDirection: TimeRelation
    ): List<MessageWrapper> {
        return if (timelineDirection == AFTER) {
            storedMessages.filter { isInFuture(it, startTimestamp, streamInfo) }
        } else {
            storedMessages.filter { isInPast(it, startTimestamp, streamInfo) }
        }
    }

    private suspend fun pullMoreMerged(
        streamsInfo: List<StreamInfo>,
        timelineDirection: TimeRelation,
        perStreamLimit: Int,
        startTimestamp: Instant,
        messageId: StoredMessageId?
    ): List<MessageWrapper> {
        logger.debug { "pulling more messages (streams=${streamsInfo} direction=$timelineDirection perStreamLimit=$perStreamLimit)" }
        return coroutineScope {
            val startIdStream = messageId?.let { Pair(it.streamName, it.direction) }
            streamsInfo.map { stream ->
             //   async {
                    val pulled =
                        pullMoreWrapped(stream.lastElement, perStreamLimit, timelineDirection, stream.isFirstPull)

                    dropUntilInRangeInOppositeDirection(startTimestamp, stream, pulled, timelineDirection).let {
                        stream.update(pulled.size, perStreamLimit, timelineDirection, it)
                        if (stream.stream == startIdStream) {
                            it.filterNot { m -> m.message.id == messageId }
                        } else {
                            it
                        }
                    }
              //  }
            }//.awaitAll()
            .flatten().let { list ->
                if (timelineDirection == AFTER) {
                    list.sortedBy { it.message.timestamp }
                } else {
                    list.sortedByDescending { it.message.timestamp }
                }
            }
        }
    }
}
