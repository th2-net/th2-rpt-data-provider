/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.rptdataprovider.handlers


import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedEventInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import com.exactpro.th2.rptdataprovider.maxInstant
import com.exactpro.th2.rptdataprovider.minInstant
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.tryToGetTestEvents
import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.client.Counter
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

class SearchEventsHandler(context: Context<*, *, *, *>) {

    companion object {
        private val logger = KotlinLogging.logger { }
        private val processedEventCount = Counter.build(
            "processed_event_count", "Count of processed Events"
        ).register()
    }

    private val cradle: CradleService = context.cradleService
    private val eventProducer: EventProducer = context.eventProducer
    private val sseEventSearchStep: Long = context.configuration.sseEventSearchStep.value.toLong()
    private val eventSearchChunkSize: Int = context.configuration.eventSearchChunkSize.value.toInt()
    private val keepAliveTimeout: Long = context.configuration.keepAliveTimeout.value.toLong()

    private suspend fun keepAlive(
        writer: StreamWriter<*, *>,
        lastScannedObjectInfo: LastScannedObjectInfo,
        counter: AtomicLong
    ) {
        while (coroutineContext.isActive) {
            writer.write(lastScannedObjectInfo, counter)
            delay(keepAliveTimeout)
        }
    }


    private suspend fun getEventsSuspend(
        request: SseEventSearchRequest,
        timestampFrom: Instant,
        timestampTo: Instant
    ): Iterable<StoredTestEvent> {
        return coroutineScope {

            val order = if (request.searchDirection == AFTER) Order.DIRECT else Order.REVERSE

            val filter = TestEventFilter.builder()
                .startTimestampFrom().isGreaterThanOrEqualTo(timestampFrom)
                .startTimestampTo().isLessThan(timestampTo)
                .order(order)
                .scope(request.scope)
                .bookId(request.bookId)
                .build()

            if (request.parentEvent != null) {
                if (request.parentEvent.batchId != null) {
                    cradle.getEventSuspend(request.parentEvent.batchId)?.let {
                        listOf(it.asBatch())
                    } ?: emptyList()
                } else {
                    cradle.getEventsSuspend(request.parentEvent.eventId, filter)
                }
            } else {
                cradle.getEventsSuspend(filter)
            }
        }
    }

    private fun prepareEvents(
        wrappers: List<StoredTestEvent>,
        request: SseEventSearchRequest,
        timestampFrom: Instant,
        timestampTo: Instant,
    ): List<BaseEventEntity> {
        return wrappers.asSequence().flatMap { entry ->
            if (entry.isBatch) {
                val batch = entry.asBatch()
                batch.tryToGetTestEvents(request.parentEvent?.eventId).mapNotNull { event ->
                    if (event.startTimestamp < timestampFrom || event.startTimestamp >= timestampTo) {
                        null
                    } else {
                        event to eventProducer.fromStoredEvent(event, batch)
                    }
                }
            } else {
                val single = entry.asSingle()
                sequenceOf(single to eventProducer.fromStoredEvent(single, null))
            }
        }.let { eventTreesNodes ->
            eventProducer.mutableFromEventsProcessed(eventTreesNodes, request)
        }.also { events ->
            // The sorting here is performed in order to avoid unexpected events order
            // when several batches were added in the same chunk we process here
            // Events between those batches might be unordered
            events.sortWith(
                Comparator.comparing(BaseEventEntity::startTimestamp)
                    .run {
                        if (request.searchDirection == AFTER) {
                            this
                        } else {
                            reversed()
                        }
                    }
            )
        }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    private suspend fun getEventFlow(
        request: SseEventSearchRequest,
        timestampFrom: Instant,
        timestampTo: Instant,
        parentContext: CoroutineContext
    ): Flow<Deferred<List<BaseEventEntity>>> {
        return coroutineScope {
            logger.info { "Requesting event from $timestampFrom to $timestampTo" }
            flow {
                val eventsCollection =
                    getEventsSuspend(request, timestampFrom, timestampTo)
                        .asSequence()
                        .chunked(eventSearchChunkSize)
                // Cradle suppose to return events in the right order depending on the order in the request
                // We don't need to sort entities between each other.
                // However, there still might be an issue with batches if it contains events for a long period of time
                // For now just keep it in mind when you are investigating the reason
                // some events returned in unexpected order
                for (event in eventsCollection)
                    emit(event)
            }
                .map { wrappers ->
                    async(parentContext) {
                        prepareEvents(wrappers, request, timestampFrom, timestampTo)
                            .also { parentContext.ensureActive() }
                    }
                }.buffer(BUFFERED)
        }
    }


    private fun changeOfDayProcessing(from: Instant, to: Instant): Iterable<Pair<Instant, Instant>> {
        return if (from.atOffset(ZoneOffset.UTC).dayOfYear != to.atOffset(ZoneOffset.UTC).dayOfYear) {
            val pivot = from.atOffset(ZoneOffset.UTC).plusDays(1).with(LocalTime.of(0, 0, 0, 0)).toInstant()
            listOf(Pair(from, pivot.minusNanos(1)), Pair(pivot, to))
        } else {
            listOf(Pair(from, to))
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
        request: SseEventSearchRequest, sseEventSearchStep: Long, initTimestamp: Instant
    ): Sequence<Pair<Instant, Instant>> {

        var timestamp = initTimestamp

        return sequence {
            val comparator = getComparator(request.searchDirection, request.endTimestamp)
            while (comparator.invoke(timestamp)) {
                yieldAll(if (request.searchDirection == AFTER) {
                    val toTimestamp = minInstant(
                        minInstant(timestamp.plusSeconds(sseEventSearchStep), Instant.MAX),
                        request.endTimestamp ?: Instant.MAX
                    )
                    changeOfDayProcessing(timestamp, toTimestamp).also { timestamp = toTimestamp }
                } else {
                    val fromTimestamp = maxInstant(
                        maxInstant(timestamp.minusSeconds(sseEventSearchStep), Instant.MIN),
                        request.endTimestamp ?: Instant.MIN
                    )
                    changeOfDayProcessing(fromTimestamp, timestamp).reversed().also { timestamp = fromTimestamp }
                })
            }
        }
    }

    private suspend fun dropByTimestampFilter(
        request: SseEventSearchRequest, resumeFromTimestamp: Instant
    ): (BaseEventEntity) -> Boolean {
        return { event: BaseEventEntity ->
            if (request.searchDirection == AFTER) {
                event.startTimestamp.isBeforeOrEqual(resumeFromTimestamp)
            } else {
                event.startTimestamp.isAfterOrEqual(resumeFromTimestamp)
            }
        }
    }


    @ExperimentalCoroutinesApi
    private suspend fun dropBeforeResumeId(
        eventFlow: Flow<BaseEventEntity>,
        resumeFromId: ProviderEventId,
    ): Flow<BaseEventEntity> {
        return flow {
            var headIsDropped = false
            eventFlow.collect {
                if (!headIsDropped) {
                    if (it.id == resumeFromId) {
                        headIsDropped = true
                    }
                } else {
                    emit(it)
                }
            }
        }
    }


    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(request: SseEventSearchRequest, writer: StreamWriter<*, *>) {
        coroutineScope {

            val lastScannedObject = LastScannedEventInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)

            val resumeProviderId = request.resumeFromId?.let(::ProviderEventId)
            val resumeTimestamp: Instant? = resumeProviderId?.let { eventProducer.resumeTimestamp(it) }
            val startTimestamp: Instant = if (resumeProviderId == null) {
                requireNotNull(request.startTimestamp) { "start timestamp must be set" }
            } else {
                requireNotNull(resumeTimestamp) { "timestamp for $resumeProviderId cannot be extracted" }
            }
            val timeIntervals = getTimeIntervals(request, sseEventSearchStep, startTimestamp)
            val parentEventCounter = IParentEventCounter.create(request.limitForParent)

            flow {
                for ((start, end) in timeIntervals) {

                    lastScannedObject.update(start)

                    getEventFlow(
                        request, start, end, currentCoroutineContext()
                    ).collect { emit(it) }

                    if (request.parentEvent?.batchId != null) break
                }
            }
                .map { it.await() }
                .flatMapConcat { it.asFlow() }
                .let { eventFlow: Flow<BaseEventEntity> ->
                    if (resumeProviderId != null) {
                        dropBeforeResumeId(eventFlow, resumeProviderId)
                    } else {
                        eventFlow
                    }
                }
                .takeWhile { event ->
                    request.endTimestamp?.let {
                        if (request.searchDirection == AFTER) {
                            event.startTimestamp.isBeforeOrEqual(it)
                        } else {
                            event.startTimestamp.isAfterOrEqual(it)
                        }
                    } ?: true
                }
                .onEach { event ->
                    lastScannedObject.update(event, scanCnt)
                    processedEventCount.inc()
                }
                .filter { request.filterPredicate.apply(it) && parentEventCounter.checkCountAndGet(it) }
                .let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive(writer, lastScannedObject, lastEventId)
                    }
                }.onCompletion {
                    currentCoroutineContext().cancelChildren()
                    it?.let { throwable -> throw throwable }
                }.let { eventFlow ->
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