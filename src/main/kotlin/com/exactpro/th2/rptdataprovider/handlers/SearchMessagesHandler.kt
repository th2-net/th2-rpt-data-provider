/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.messageProfile.MessageProfile
import com.exactpro.th2.rptdataprovider.messageProfile.ProfilesStatistics
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import com.fasterxml.jackson.databind.ObjectMapper
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.lang.Integer.min
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

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

    var messagesProfilesMap: MutableMap<StoredMessageId, MessageProfile> = mutableMapOf()

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
        messageId: StoredMessageId?,
        timelineDirection: TimeRelation,
        timestampFrom: Instant?,
        timestampTo: Instant?
    ): Instant {
        return messageId?.let {
            cradle.getMessageSuspend(it)?.timestamp
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

    private suspend fun initStreamsInfoFromIds(request: SseMessageSearchRequest): MutableList<StreamInfo> {
        return coroutineScope {
            mutableListOf<StreamInfo>().apply {
                request.resumeFromIdsList!!.forEach {
                    val timestamp = chooseStartTimestamp(
                        it, request.searchDirection,
                        request.startTimestamp, request.endTimestamp
                    )
                    add(
                        StreamInfo(
                            Pair(it.streamName, it.direction), it, timestamp, request.keepOpen, timestamp, false
                        )
                    )
                }
            }
        }
    }


    private suspend fun initStreamsInfoFromTime(
        timelineDirection: TimeRelation,
        streamList: List<String>?,
        timestamp: Instant,
        keepOpen: Boolean = false
    ): MutableList<StreamInfo> {
        return mutableListOf<StreamInfo>().apply {
            for (stream in streamList ?: emptyList()) {
                for (direction in Direction.values()) {
                    val storedMessageId =
                        getFirstMessageIdDifferentDays(timestamp, stream, direction, timelineDirection)

                    if (storedMessageId != null) {
                        val messageBatch = cradle.getMessageBatchSuspend(storedMessageId)
                        val nearestMessage = getNearestMessage(messageBatch, timelineDirection, timestamp)
                        add(
                            StreamInfo(
                                Pair(stream, direction), nearestMessage?.id,
                                nearestMessage?.timestamp, keepOpen, timestamp
                            )
                        )
                    } else {
                        add(StreamInfo(Pair(stream, direction), null, null, keepOpen, timestamp))
                    }
                }
            }
        }
    }

    private suspend fun initStreamsInfo(request: SseMessageSearchRequest): MutableList<StreamInfo> {
        return if (request.resumeFromIdsList.isNullOrEmpty()) {
            val startTimestamp = chooseStartTimestamp(
                request.resumeFromId?.let { StoredMessageId.fromString(it) },
                request.searchDirection,
                request.startTimestamp, request.startTimestamp
            )
            initStreamsInfoFromTime(request.searchDirection, request.stream, startTimestamp, request.keepOpen)
        } else {
            initStreamsInfoFromIds(request)
        }
    }


    @ExperimentalCoroutinesApi
    @FlowPreview
    private suspend fun getMessageStream(
        request: SseMessageSearchRequest,
        streamsInfo: MutableList<StreamInfo>,
        initLimit: Int,
        messageId: StoredMessageId?,
        parentContext: CoroutineScope
    ): Flow<MessageWrapper> {
        return coroutineScope {
            flow {
                var limit = min(maxMessagesLimit, initLimit)
                var isSearchInFuture = false
                do {
                    if (isSearchInFuture)
                        delay(sseSearchDelay * 1000)

                    streamsInfo.sortBy { it.lastElementTime }

                    val data = pullMoreMerged(
                        streamsInfo, request,
                        limit, messageId, parentContext
                    )

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
        keepAlive: suspend (StreamWriter, lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) -> Unit,
        writer: StreamWriter
    ) {
        withContext(coroutineContext) {
            val startMessageCountLimit = 25
            val lastScannedObject = LastScannedObjectInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)
            val messageIdStored = request.resumeFromId?.let { StoredMessageId.fromString(it) }
            var streamsInfo: MutableList<StreamInfo> = mutableListOf()
            var lastIdInStream: MutableMap<Pair<String, Direction>, StoredMessageId?> = mutableMapOf()

            var profilesStatistics: ProfilesStatistics? = ProfilesStatistics()

            flow {

                streamsInfo = initStreamsInfo(request)
                lastIdInStream = streamsInfo.associate { it.stream to it.lastElement } as MutableMap

                getMessageStream(
                    request, streamsInfo, request.resultCountLimit ?: startMessageCountLimit,
                    messageIdStored, this@withContext
                ).collect {
                    messagesProfilesMap[it.id]!!.gotMessage()
                    emit(it)
                }
            }.map {
                async {
                    val parsedMessage = it.getParsedMessage()
                    messagesProfilesMap[it.id]!!.parsed(parsedMessage)
                    messageCache.put(parsedMessage.messageId, parsedMessage)
                    Pair(parsedMessage, request.filterPredicate.apply(parsedMessage))
                }.also { coroutineContext.ensureActive() }
            }
                .buffer(messageSearchPipelineBuffer)
                .map { it.await() }
                .onEach { (message, _) ->
                    lastScannedObject.update(message, scanCnt)
                    message.id.let {
                        lastIdInStream[Pair(it.streamName, it.direction)] = it
                    }
                    processedMessageCount.inc()
                    messagesProfilesMap[message.id]!!.filtered(message)
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
                    writer.write(lastIdInStream)
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()
                    writer.write(it, lastEventId)
                    if (!profilesStatistics!!.isStarted) {
                        profilesStatistics.start()
                    }
                    var messageProfile = messagesProfilesMap[it.id]!!.endOfProcessing()
                    profilesStatistics!!.processedMessage(messageProfile)
                }
            profilesStatistics!!.enough()
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
        request: SseMessageSearchRequest,
        isFirstPull: Boolean,
        searchContext: CoroutineScope
    ): List<MessageWrapper> {
        var messageWrapper: MessageWrapper
        return coroutineScope {
            databaseRequestRetry(dbRetryDelay) {
                pullMore(startId, limit, request.searchDirection, isFirstPull)
            }.map { batch ->
                async {
                    val parsedFuture = searchContext.async {
                        messageProducer.fromRawMessage(batch, request)
                    }
                    batch.messages.map { message ->
                        messageWrapper = MessageWrapper.build(message, parsedFuture)
                        var messageProfile = MessageProfile(message.id, messageWrapper)
                        messagesProfilesMap[message.id] = messageProfile
                        messageWrapper
                    }
                }
            }.awaitAll().flatten()
        }
    }

    private fun isInFuture(wrapper: MessageWrapper, streamInfo: StreamInfo): Boolean {
        return wrapper.message.timestamp.isAfterOrEqual(streamInfo.startTimestamp) &&
                streamInfo.lastElement?.let {
                    if (streamInfo.isFirstPull) {
                        wrapper.message.index >= it.index
                    } else {
                        wrapper.message.index > it.index
                    }
                } ?: true
    }

    private fun isInPast(wrapper: MessageWrapper, streamInfo: StreamInfo): Boolean {
        return wrapper.message.timestamp.isBeforeOrEqual(streamInfo.startTimestamp) &&
                streamInfo.lastElement?.let {
                    if (streamInfo.isFirstPull) {
                        wrapper.message.index <= it.index
                    } else {
                        wrapper.message.index < it.index
                    }
                } ?: true
    }

    private fun dropUntilInRangeInOppositeDirection(
        streamInfo: StreamInfo,
        storedMessages: List<MessageWrapper>,
        timelineDirection: TimeRelation
    ): List<MessageWrapper> {
        return if (timelineDirection == AFTER) {
            storedMessages.filter { isInFuture(it, streamInfo) }
        } else {
            storedMessages.filter { isInPast(it, streamInfo) }
        }
    }

    private suspend fun pullMoreMerged(
        streamsInfo: List<StreamInfo>,
        request: SseMessageSearchRequest,
        perStreamLimit: Int,
        messageId: StoredMessageId?,
        parentContext: CoroutineScope
    ): List<MessageWrapper> {
        logger.debug { "pulling more messages (streams=${streamsInfo} direction=${request.searchDirection} perStreamLimit=$perStreamLimit)" }
        return coroutineScope {
            val startIdStream = messageId?.let { Pair(it.streamName, it.direction) }
            streamsInfo.flatMap { stream ->
                val pulled = pullMoreWrapped(
                    stream.lastElement, perStreamLimit, request,
                    stream.isFirstPull, parentContext
                )
                dropUntilInRangeInOppositeDirection(stream, pulled, request.searchDirection).let {
                    stream.update(pulled.size, perStreamLimit, request.searchDirection, it)
                    if (stream.stream == startIdStream) {
                        it.filterNot { m -> m.message.id == messageId }
                    } else {
                        it
                    }
                }
            }.let { list ->
                if (request.searchDirection == AFTER) {
                    list.sortedBy { it.message.timestamp }
                } else {
                    list.sortedByDescending { it.message.timestamp }
                }
            }
        }
    }
}
