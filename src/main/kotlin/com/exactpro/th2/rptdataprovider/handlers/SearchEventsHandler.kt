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
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedEventInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
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
    private val eventSearchChunkSize: Int = context.configuration.eventSearchChunkSize.value.toInt()
    private val keepAliveTimeout: Long = context.configuration.keepAliveTimeout.value.toLong()


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


    private suspend fun keepAlive(
        writer: StreamWriter,
        lastScannedObjectInfo: LastScannedObjectInfo,
        counter: AtomicLong
    ) {
        while (coroutineContext.isActive) {
            writer.write(lastScannedObjectInfo, counter)
            delay(keepAliveTimeout)
        }
    }


    private suspend fun getEventsSuspend(
        parentEvent: ProviderEventId?,
        timestampFrom: Instant,
        timestampTo: Instant,
        searchDirection: TimeRelation
    ): Iterable<StoredTestEventMetadata> {
        return coroutineScope {
            val order = if (searchDirection == AFTER) Order.DIRECT else Order.REVERSE
            if (parentEvent != null) {
                if (parentEvent.batchId != null) {
                    cradle.getEventSuspend(parentEvent.batchId)?.let {
                        listOf(StoredTestEventMetadata(it.asBatch()))
                    } ?: emptyList()
                } else {
                    cradle.getEventsSuspend(parentEvent.eventId, timestampFrom, timestampTo, order)
                }
            } else {
                cradle.getEventsSuspend(timestampFrom, timestampTo, order)
            }
        }
    }


    private suspend fun prepareNonBatchedEvent(
        metadata: List<StoredTestEventMetadata>,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        return metadata.map {
            eventProducer.fromEventMetadata(it, null)
        }.let { eventTreesNodes ->
            eventProducer.fromSingleEventsProcessed(eventTreesNodes, request)
        }
    }


    private suspend fun prepareBatchedEvent(
        metadata: List<StoredTestEventMetadata>,
        timestampFrom: Instant,
        timestampTo: Instant,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        return metadata.map { it to it.tryToGetTestEvents(request.parentEvent?.eventId) }.let { eventsWithBatch ->
            eventsWithBatch.map { (batch, events) ->
                batch.id to events?.map {
                    eventProducer.fromEventMetadata(StoredTestEventMetadata(it), batch)
                }
            }.filter { it.second?.isNotEmpty() ?: true }
                .let { eventTreeNodes ->
                    val notNullEvents = eventTreeNodes.mapNotNull { (batch, events) ->
                        events?.let { batch to it }
                    }
                    val nullEvents = eventTreeNodes.filter { it.second == null }

                    val parsedEvents = eventProducer.fromBatchIdsProcessed(notNullEvents, request)

                    parsedEvents.toMutableList().apply {
                        addAll(
                            nullEvents.flatMap { (batch, _) ->
                                getDirectBatchedChildren(batch, timestampFrom, timestampTo, request)
                            }
                        )
                    }
                }
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
            flow {
                val eventsCollection =
                    getEventsSuspend(request.parentEvent, timestampFrom, timestampTo, request.searchDirection)
                        .asSequence().chunked(eventSearchChunkSize)

                for (event in eventsCollection)
                    emit(event)
            }
                .map { metadata ->
                    async(parentContext) {
                        metadata.groupBy { it.isBatch }.flatMap { entry ->
                            if (entry.key) {
                                prepareBatchedEvent(
                                    entry.value, timestampFrom, timestampTo, request
                                )
                            } else {
                                prepareNonBatchedEvent(entry.value, request)
                            }
                        }.let { events ->
                            if (request.searchDirection == AFTER) events.sortedBy { it.startTimestamp }
                            else events.sortedByDescending { it.startTimestamp }
                        }.also { parentContext.ensureActive() }
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
        request: SseEventSearchRequest, resumeFromEvent: BaseEventEntity
    ): (BaseEventEntity) -> Boolean {
        return { event: BaseEventEntity ->
            if (request.searchDirection == AFTER) {
                event.startTimestamp.isBeforeOrEqual(resumeFromEvent.startTimestamp)
            } else {
                event.startTimestamp.isAfterOrEqual(resumeFromEvent.startTimestamp)
            }
        }
    }


    @ExperimentalCoroutinesApi
    private suspend fun dropBeforeResumeId(
        eventFlow: Flow<BaseEventEntity>,
        request: SseEventSearchRequest,
        resumeFromEvent: BaseEventEntity
    ): Flow<BaseEventEntity> {
        return flow {
            val dropByTimestamp = dropByTimestampFilter(request, resumeFromEvent)
            val head = mutableListOf<BaseEventEntity>()
            var headIsDropped = false
            eventFlow.collect {
                if (!headIsDropped) {
                    when {
                        dropByTimestamp(it) && it.id != resumeFromEvent.id -> head.add(it)
                        it.id == resumeFromEvent.id -> headIsDropped = true
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


    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(request: SseEventSearchRequest, writer: StreamWriter) {
        coroutineScope {

            val lastScannedObject = LastScannedEventInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)

            val resumeFromEvent = request.resumeFromId?.let {
                eventProducer.fromId(ProviderEventId(it))
            }
            val startTimestamp = resumeFromEvent?.startTimestamp ?: request.startTimestamp!!
            val timeIntervals = getTimeIntervals(request, sseEventSearchStep, startTimestamp)
            val parentEventCounter = ParentEventCounter(request.limitForParent)

            flow {
                for (timestamp in timeIntervals) {

                    lastScannedObject.update(timestamp.first)

                    getEventFlow(
                        request, timestamp.first, timestamp.second, coroutineContext
                    ).collect { emit(it) }

                    if (request.parentEvent?.batchId != null) break
                }
            }
                .map { it.await() }
                .flatMapConcat { it.asFlow() }
                .let { eventFlow: Flow<BaseEventEntity> ->
                    if (resumeFromEvent != null) {
                        dropBeforeResumeId(eventFlow, request, resumeFromEvent)
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
                .filter { request.filterPredicate.apply(it) }
                .let {
                    if (parentEventCounter.limitForParent != null) {
                        it.filter { event -> parentEventCounter.checkCountAndGet(event) != null }
                    } else {
                        it
                    }
                }
                .let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive(writer, lastScannedObject, lastEventId)
                    }
                }.onCompletion {
                    coroutineContext.cancelChildren()
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


    // this is a fallback that should be deprecated after migration to cradle 1.6
    private suspend fun getDirectBatchedChildren(
        batchId: StoredTestEventId,
        timestampFrom: Instant,
        timestampTo: Instant,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        val batch = cradle.getEventSuspend(batchId)?.asBatch()

        return (batch?.testEvents
            ?: throw CradleEventNotFoundException("unable to get test events of batch '$batchId'"))
            .filter { it.startTimestamp.isAfter(timestampFrom) && it.startTimestamp.isBefore(timestampTo) }
            .filter { request.parentEvent?.eventId?.let { id -> it.parentId == id } ?: true }
            .map { testEvent ->
                eventProducer.fromStoredEvent(testEvent, batch)
            }.let { events ->
                eventProducer.fromBatchIdsProcessed(listOf(batch.id to events), request)
            }
    }
}
