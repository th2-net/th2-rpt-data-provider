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


import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.testevents.BatchedStoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.dataprovider.grpc.Events
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.internal.IntermediateEvent
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedEventInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

class SearchEventsHandler(private val context: Context) {

    companion object {
        private val logger = KotlinLogging.logger { }
        private val processedEventCount = Counter.build(
            "processed_event_count", "Count of processed Events"
        ).register()
    }

    private val cradle: CradleService = context.cradleService
    private val eventProducer: EventProducer = context.eventProducer
    private val sseEventSearchStep: Long = context.configuration.sseEventSearchStep.value.toLong()
    private val keepAliveTimeout: Long = context.configuration.keepAliveTimeout.value.toLong()
    private val eventSearchGap: Long = context.configuration.eventSearchGap.value.toLong()


    private data class ParentEventCounter private constructor(
        private val parentEventCounter: ConcurrentHashMap<String, AtomicLong>?,
        val limitForParent: Long?
    ) {

        constructor(limitForParent: Long?) : this(
            parentEventCounter = limitForParent?.let { ConcurrentHashMap<String, AtomicLong>() },
            limitForParent = limitForParent
        )

        fun checkCountAndGet(event: BaseEventEntity): BaseEventEntity? {
            if (limitForParent == null || event.parentEventId == null)
                return event

            return parentEventCounter!!.getOrPut(event.parentEventId.toString(), { AtomicLong(1) }).let { parentCount ->
                if (parentCount.get() <= limitForParent) {
                    parentCount.incrementAndGet()
                    event
                } else {
                    parentEventCounter.putIfAbsent(event.id.toString(), AtomicLong(Long.MAX_VALUE))
                    null
                }
            }
        }
    }


    private suspend fun keepAlive(writer: StreamWriter, info: LastScannedObjectInfo, counter: AtomicLong) {
        while (coroutineContext.isActive) {
            writer.write(info, counter)
            delay(keepAliveTimeout)
        }
    }


    private data class SearchInterval(
        val start: Instant,
        val end: Instant,
        val resumeId: ProviderEventId? = null,
        private val startWithGap: Instant? = null
    ) {
        val requestStart: Instant = startWithGap ?: start
        val requestEnd: Instant = end
    }


    private fun getDirection(searchDirection: TimeRelation): Order {
        return if (searchDirection == AFTER) {
            Order.DIRECT
        } else {
            Order.REVERSE
        }
    }


    private suspend fun getEventsSuspend(
        request: SseEventSearchRequest,
        searchInterval: SearchInterval
    ): Iterable<StoredTestEventWrapper> {
        return coroutineScope {

            val parentEvent = request.parentEvent
            val order = getDirection(request.searchDirection)

            if (parentEvent != null) {
                if (parentEvent.batchId != null) {
                    cradle.getEventSuspend(parentEvent.batchId)?.let {
                        listOf(StoredTestEventWrapper(it.asBatch()))
                    } ?: emptyList()
                } else {
                    cradle.getEventsSuspend(
                        searchInterval.requestStart, searchInterval.requestEnd,
                        parentEvent.eventId, searchInterval.resumeId?.eventId, order
                    )
                }
            } else {
                cradle.getEventsSuspend(
                    searchInterval.requestStart, searchInterval.requestEnd,
                    order, searchInterval.resumeId?.eventId
                )
            }
        }
    }


    private fun trimLastBatch(
        events: List<IntermediateEvent>,
        searchInterval: SearchInterval,
        direction: TimeRelation
    ): List<IntermediateEvent> {
        return if (direction == AFTER) {
            events.dropLastWhile {
                it.baseEventEntity.startTimestamp.isAfterOrEqual(searchInterval.end)
            }
        } else {
            events.dropLastWhile {
                it.baseEventEntity.startTimestamp.isBeforeOrEqual(searchInterval.start)
            }
        }
    }


    private fun prepareEvents(
        wrapper: StoredTestEventWrapper,
        request: SseEventSearchRequest,
        searchInterval: SearchInterval,
        lastBatch: Boolean
    ): List<BaseEventEntity> {

        val baseEventEntities = if (wrapper.isBatch) {
            val batch = wrapper.asBatch()
            batch.tryToGetTestEvents(request.parentEvent?.eventId).map { event ->
                IntermediateEvent(event, eventProducer.fromStoredEvent(event, batch))
            }
        } else {
            val single = wrapper.asSingle()
            listOf(IntermediateEvent(single, eventProducer.fromStoredEvent(single, null)))
        }

        val filteredEventEntities = orderedByDirection(baseEventEntities, request.searchDirection).let {
            if (lastBatch) trimLastBatch(it, searchInterval, request.searchDirection) else it
        }

        return eventProducer.fromEventsProcessed(filteredEventEntities, request)
    }


    private fun <T> orderedByDirection(iterable: List<T>, timeRelation: TimeRelation): List<T> {
        return if (timeRelation == AFTER) {
            iterable
        } else {
            iterable.reversed()
        }
    }


    @FlowPreview
    @ExperimentalCoroutinesApi
    private suspend fun getEventFlow(
        request: SseEventSearchRequest,
        searchInterval: SearchInterval,
        parentContext: CoroutineContext
    ): Flow<Deferred<Iterable<BaseEventEntity>>> {
        return coroutineScope {
            flow {
                val eventsCollection = getEventsSuspend(request, searchInterval).iterator()

                while (eventsCollection.hasNext()) {
                    val event = eventsCollection.next()

                    if (eventsCollection.hasNext()) {
                        emit(event to false)
                    } else {
                        emit(event to true)
                    }
                }
            }
                .map { (wrappers, lastBatch) ->
                    async(parentContext) {
                        prepareEvents(wrappers, request, searchInterval, lastBatch).also {
                            parentContext.ensureActive()
                        }
                    }
                }
                .buffer(BUFFERED)
        }
    }


    private fun changeOfDayProcessing(from: Instant, to: Instant): Iterable<SearchInterval> {
        val startCurrentDay = getDayStart(from)

        val startWithGap = maxInstant(startCurrentDay.toInstant(), from.minusSeconds(eventSearchGap))

        return if (isDifferentDays(from, to)) {
            val pivot = startCurrentDay
                .plusDays(1)
                .toInstant()

            listOf(SearchInterval(from, pivot.minusNanos(1), null, startWithGap), SearchInterval(pivot, to))
        } else {
            listOf(SearchInterval(from, to, null, startWithGap))
        }
    }


    private fun getComparator(searchDirection: TimeRelation, endTimestamp: Instant?): (Instant) -> Boolean {
        return if (searchDirection == AFTER) {
            { timestamp: Instant -> timestamp.isBefore(endTimestamp ?: Instant.MAX) }
        } else {
            { timestamp: Instant -> timestamp.isAfter(endTimestamp ?: Instant.MIN) }
        }
    }


    private fun getTimeIntervals(
        initTimestamp: Instant,
        request: SseEventSearchRequest,
        resumeId: ProviderEventId?
    ): Sequence<SearchInterval> {

        var currentTimestamp = initTimestamp

        return sequence {
            val comparator = getComparator(request.searchDirection, request.endTimestamp)
            while (comparator.invoke(currentTimestamp)) {
                val timeIntervals = if (request.searchDirection == AFTER) {
                    val toTimestamp = minInstant(
                        minInstant(currentTimestamp.plus(1, ChronoUnit.DAYS), Instant.MAX),
                        request.endTimestamp ?: Instant.MAX
                    )
                    changeOfDayProcessing(currentTimestamp, toTimestamp).also {
                        currentTimestamp = toTimestamp
                    }
                } else {
                    val fromTimestamp = maxInstant(
                        maxInstant(currentTimestamp.minus(1, ChronoUnit.DAYS), Instant.MIN),
                        request.endTimestamp ?: Instant.MIN
                    )
                    changeOfDayProcessing(fromTimestamp, currentTimestamp)
                        .reversed()
                        .also { currentTimestamp = fromTimestamp }
                }
                yieldAll(timeIntervals)
            }
        }.mapIndexed { i, interval ->
            if (i == 0) {
                interval.copy(resumeId = resumeId)
            } else {
                interval
            }
        }
    }


    private fun dropByTimestampFilter(direction: TimeRelation, startTimestamp: Instant): (BaseEventEntity) -> Boolean {
        return { event: BaseEventEntity ->
            if (direction == AFTER) {
                event.startTimestamp.isBeforeOrEqual(startTimestamp)
            } else {
                event.startTimestamp.isAfterOrEqual(startTimestamp)
            }
        }
    }


    @ExperimentalCoroutinesApi
    private suspend fun dropBeforeResumeId(
        eventFlow: Flow<BaseEventEntity>,
        searchDirection: TimeRelation,
        startTimestamp: Instant,
        resumeId: ProviderEventId
    ): Flow<BaseEventEntity> {
        return flow {
            val dropByTimestamp = dropByTimestampFilter(searchDirection, startTimestamp)

            val head = mutableListOf<BaseEventEntity>()
            var headIsDropped = false
            eventFlow.collect {
                if (!headIsDropped) {
                    when {
                        dropByTimestamp(it) && it.id != resumeId -> head.add(it)
                        it.id == resumeId -> headIsDropped = true
                        else -> {
                            emitAll(head.asFlow())
                            emit(it)
                            headIsDropped = true
                        }
                    }
                } else {
                    emit(it)
                }
            }
        }
    }


    private fun getStartTimestamp(event: StoredTestEventWrapper?, request: SseEventSearchRequest): Instant {
        return event?.let {
            if (request.searchDirection == AFTER) {
                it.startTimestamp
            } else {
                it.endTimestamp
            }
        } ?: request.startTimestamp!!
    }


    private fun isBefore(event: BaseEventEntity, instant: Instant, searchDirection: TimeRelation): Boolean {
        return if (searchDirection == AFTER) {
            event.startTimestamp.isBefore(instant)
        } else {
            event.startTimestamp.isAfter(instant)
        }
    }


    private fun getEvent(event: StoredTestEventWrapper, providerEventId: ProviderEventId): StoredTestEventWithContent {
        return if (event.isBatch) {
            val batch = event.asBatch()
            batch.getTestEvent(providerEventId.eventId)
        } else {
            event.asSingle()
        }
    }


    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(request: SseEventSearchRequest, writer: StreamWriter) {
        coroutineScope {

            val lastScannedObject = LastScannedEventInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)

            val resumeId = request.resumeFromId?.let { ProviderEventId(it) }
            val eventWrapper = resumeId?.let { eventProducer.getEventWrapper(it) }

            val startTimestamp = getStartTimestamp(eventWrapper, request)

            val resumeEvent = eventWrapper?.let { getEvent(it, resumeId) }

            val timeIntervals = getTimeIntervals(
                startTimestamp, request, resumeId
            ).toList()

            val parentEventCounter = ParentEventCounter(request.limitForParent)

            flow {
                for (timestamp in timeIntervals) {

                    lastScannedObject.update(timestamp.start)

                    getEventFlow(
                        request, timestamp, coroutineContext
                    ).collect { emit(it) }

                    if (request.parentEvent?.batchId != null) break
                }
            }
                .map { it.await() }
                .flatMapConcat { it.asFlow() }
                .let { eventFlow: Flow<BaseEventEntity> ->
                    if (resumeEvent != null) {
                        dropBeforeResumeId(eventFlow, request.searchDirection, resumeEvent.startTimestamp, resumeId)
                    } else {
                        eventFlow.dropWhile { event ->
                            isBefore(event, startTimestamp, request.searchDirection)
                        }
                    }
                }
                .onEach { event ->
                    lastScannedObject.update(event, scanCnt)
                    processedEventCount.inc()
                }
                .filter { request.filterPredicate.apply(it) }
                .let {
                    if (parentEventCounter.limitForParent != null) {
                        it.filter { event -> parentEventCounter.checkCountAndGet(event) != null }
                    } else {
                        it
                    }
                }
                .let { eventFlow ->
                    request.resultCountLimit?.let { eventFlow.take(it) } ?: eventFlow
                }
                .onStart {
                    launch {
                        keepAlive(writer, lastScannedObject, lastEventId)
                    }
                }
                .onCompletion {
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .let { eventFlow ->
                    if (request.metadataOnly) {
                        eventFlow.collect {
                            coroutineContext.ensureActive()
                            writer.write(it.convertToEventTreeNode(), lastEventId)
                        }
                    } else {
                        eventFlow.collect {
                            coroutineContext.ensureActive()
                            writer.write(it.convertToEvent(), lastEventId)
                        }
                    }
                }
        }
    }
}
