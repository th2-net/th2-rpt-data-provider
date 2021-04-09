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


import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventBatchMetadata
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.utils.io.errors.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.flow.*
import io.prometheus.client.Counter
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import mu.KotlinLogging
import java.io.Writer
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

class SearchEventsHandler(
    private val cradle: CradleService,
    private val eventProducer: EventProducer,
    private val dbRetryDelay: Long,
    private val sseSearchDelay: Long
) {
    companion object {
        private val logger = KotlinLogging.logger { }
        private val processedEventCount = Counter.build(
            "processed_event_count", "Count of processed Events"
        ).register()
    }

    private data class ParentEventCounter private constructor(
        private val parentEventCounter: ConcurrentHashMap<String, AtomicLong>?,
        val limitForParent: Long?
    ) {

        constructor(limitForParent: Long?) : this(
            parentEventCounter = limitForParent?.let { ConcurrentHashMap<String, AtomicLong>() },
            limitForParent = limitForParent
        )

        fun checkCountAndGet(eventTreeNode: EventTreeNode): EventTreeNode? {
            if (limitForParent == null || eventTreeNode.parentId == null) return eventTreeNode
            val count = parentEventCounter!![eventTreeNode.parentId!!]
            return if (count?.let { it.get() < limitForParent } != false) {
                eventTreeNode
            } else {
                null
            }
        }

        fun update(event: EventTreeNode) {
            if (limitForParent == null) return

            event.parentId?.let { parentId ->
                parentEventCounter!!.getOrPut(parentId, { AtomicLong(0) }).also {
                    it.incrementAndGet()
                }
            }
        }
    }


    private suspend fun getEventsSuspend(
        parentEvent: String?,
        timestampFrom: Instant,
        timestampTo: Instant
    ): Iterable<StoredTestEventMetadata> {
        return coroutineScope {
            if (parentEvent != null) {
                cradle.getEventsSuspend(
                    ProviderEventId(parentEvent).eventId,
                    timestampFrom,
                    timestampTo
                )
            } else {
                cradle.getEventsSuspend(timestampFrom, timestampTo)
            }
        }
    }

    private suspend fun prepareNonBatchedEvent(
        metadata: StoredTestEventMetadata,
        parentEventCounter: ParentEventCounter
    ): List<Pair<EventTreeNode, Event?>> {
        return EventTreeNode(null, metadata).let { eventTreeNode ->
            parentEventCounter.checkCountAndGet(eventTreeNode)?.let {
                listOf(Pair(it, eventProducer.fromId(ProviderEventId(null, metadata.id))))
            } ?: emptyList()
        }
    }

    private suspend fun tryToGetTestEvents(metadata: StoredTestEventMetadata):
            Collection<BatchedStoredTestEventMetadata>? {
        return try {
            metadata.batchMetadata
        } catch (e: IOException) {
            null
        }?.testEvents
    }

    private suspend fun prepareBatchedEvent(
        metadata: StoredTestEventMetadata,
        parentEventCounter: ParentEventCounter
    ): List<Pair<EventTreeNode, Event?>>? {
        return tryToGetTestEvents(metadata)?.mapNotNull { event ->
            parentEventCounter.checkCountAndGet(
                EventTreeNode(metadata.batchMetadata, event)
            )?.let {
                Pair(it, eventProducer.fromId(ProviderEventId(event.batchId, event.id)))
            }
        }
    }

    @ExperimentalCoroutinesApi
    private suspend fun getEventFlow(
        parentEvent: String?,
        timestampFrom: Instant,
        timestampTo: Instant,
        parentContext: CoroutineContext,
        parentEventCounter: ParentEventCounter
    ): Flow<Deferred<List<Pair<EventTreeNode, Event?>>>> {
        return coroutineScope {
            flow {
                val eventsCollection =
                    databaseRequestRetry(dbRetryDelay) { getEventsSuspend(parentEvent, timestampFrom, timestampTo) }

                for (event in eventsCollection)
                    emit(event)
            }
                .map { metadata ->
                    async(parentContext) {
                        if (metadata.isBatch) {
                            prepareBatchedEvent(metadata, parentEventCounter)
                                ?: getDirectBatchedChildren(metadata.id, timestampFrom, timestampTo, parentEventCounter)
                        } else {
                            prepareNonBatchedEvent(metadata, parentEventCounter)
                        }
                    }.also { parentContext.ensureActive() }
                }
                .buffer(BUFFERED)
        }
    }


    private fun changeOfDayProcessing(from: Instant, to: Instant): Iterable<Pair<Instant, Instant>> {
        return if (from.atOffset(ZoneOffset.UTC).dayOfYear != to.atOffset(ZoneOffset.UTC).dayOfYear) {
            val pivot = from.atOffset(ZoneOffset.UTC)
                .plusDays(1).with(LocalTime.of(0, 0, 0, 0))
                .toInstant()
            listOf(Pair(from, pivot.minusNanos(1)), Pair(pivot, to))
        } else {
            listOf(Pair(from, to))
        }
    }

    private fun getComparator(searchDirection: TimeRelation, endTimestamp: Instant?): (Instant) -> Boolean {
        return if (searchDirection == TimeRelation.AFTER) {
            { timestamp: Instant -> timestamp.isBefore(endTimestamp ?: Instant.MAX) }
        } else {
            { timestamp: Instant -> timestamp.isAfter(endTimestamp ?: Instant.MIN) }
        }
    }

    private fun getTimeIntervals(
        request: SseEventSearchRequest,
        sseEventSearchStep: Long,
        initTimestamp: Instant
    ): Sequence<Pair<Instant, Instant>> {

        var timestamp = initTimestamp

        return sequence {
            val comparator = getComparator(request.searchDirection, request.endTimestamp)
            while (comparator.invoke(timestamp)) {
                yieldAll(
                    if (request.searchDirection == TimeRelation.AFTER) {
                        val toTimestamp = minInstant(timestamp.plusSeconds(sseEventSearchStep), Instant.MAX)
                        changeOfDayProcessing(timestamp, toTimestamp).also { timestamp = toTimestamp }
                    } else {
                        val fromTimestamp = maxInstant(timestamp.minusSeconds(sseEventSearchStep), Instant.MIN)
                        changeOfDayProcessing(fromTimestamp, timestamp).reversed()
                            .also { timestamp = fromTimestamp }
                    }
                )
            }
        }
    }


    private suspend fun getStartTimestamp(request: SseEventSearchRequest): Instant {
        return request.resumeFromId?.let {
            eventProducer.fromId(ProviderEventId(request.resumeFromId)).startTimestamp
        } ?: request.startTimestamp!!
    }

    private suspend fun getNextTimestampGenerator(
        request: SseEventSearchRequest,
        sseEventSearchStep: Long,
        initTimestamp: Instant
    ): Sequence<Pair<Boolean, Pair<Instant, Instant>>> {

        var isSearchInFuture = false
        val isSearchNext = request.searchDirection == TimeRelation.AFTER
        var timeIntervals = getTimeIntervals(request, sseEventSearchStep, initTimestamp).iterator()

        return sequence {
            while (timeIntervals.hasNext()) {
                val timestamp = timeIntervals.next()
                if (!isSearchInFuture && isSearchNext && timestamp.second.isAfter(Instant.now())) {
                    if (request.keepOpen) {
                        timeIntervals = getTimeIntervals(request, sseSearchDelay, timestamp.first).iterator()
                        isSearchInFuture = true
                        continue
                    } else {
                        return@sequence
                    }
                }
                yield(isSearchInFuture to timestamp)
            }
        }
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(
        request: SseEventSearchRequest,
        jacksonMapper: ObjectMapper,
        sseEventSearchStep: Long,
        keepAlive: suspend (Writer, LastScannedObjectInfo, AtomicLong) -> Unit,
        writer: Writer
    ) {
        coroutineScope {
            val lastScannedObject = LastScannedObjectInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)

            val startTimestamp = getStartTimestamp(request)
            val timeIntervals = getNextTimestampGenerator(request, sseEventSearchStep, startTimestamp)
            val parentEventCounter = ParentEventCounter(request.limitForParent)

            flow {
                for ((isSearchInFuture, timestamp) in timeIntervals) {
                    if (isSearchInFuture)
                        delay(sseSearchDelay * 1000)

                    getEventFlow(
                        request.parentEvent, timestamp.first,
                        timestamp.second, coroutineContext,
                        parentEventCounter
                    ).collect { emit(it) }
                }
            }
                .map { it.await() }
                .flatMapMerge { it.asFlow() }
                .filter { it.first.eventId != request.resumeFromId }
                .takeWhile { pair ->
                    request.endTimestamp?.let {
                        if (request.searchDirection == TimeRelation.AFTER) {
                            pair.first.startTimestamp.isBeforeOrEqual(it)
                        } else {
                            pair.first.startTimestamp.isAfterOrEqual(it)
                        }
                    } ?: true
                }
                .onEach { (event, _) ->
                    lastScannedObject.update(event, scanCnt)
                    processedEventCount.inc()
                }
                .filter { request.filterPredicate.apply(it.second!!) }
                .let {
                    if (parentEventCounter.limitForParent != null) {
                        it.onEach { (event, _) -> parentEventCounter.update(event) }
                    } else {
                        it
                    }
                }.let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive.invoke(writer, lastScannedObject, lastEventId)
                    }
                }.onCompletion {
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }.collect {
                    coroutineContext.ensureActive()
                    writer.eventWrite(SseEvent.build(jacksonMapper, it.first, lastEventId))
                }
        }
    }


    // this is a fallback that should be deprecated after migration to cradle 1.6
    private suspend fun getDirectBatchedChildren(
        batchId: StoredTestEventId,
        timestampFrom: Instant,
        timestampTo: Instant,
        parentEventCounter: ParentEventCounter
    ): List<Pair<EventTreeNode, Event?>> {

        val batch = cradle.getEventSuspend(batchId)?.asBatch()

        return (batch?.testEvents
            ?: throw CradleEventNotFoundException("unable to get test events of batch '$batchId'"))
            .filter { it.startTimestamp.isAfter(timestampFrom) && it.startTimestamp.isBefore(timestampTo) }
            .mapNotNull { testEvent ->
                val batchMetadata = StoredTestEventBatchMetadata(batch)

                val eventTreeNode = EventTreeNode(
                    batchMetadata,
                    BatchedStoredTestEventMetadata(testEvent, batchMetadata)
                )
                parentEventCounter.checkCountAndGet(eventTreeNode)?.let {
                    Pair(it, eventProducer.fromId(ProviderEventId(testEvent.batchId, testEvent.id)))
                }
            }
    }
}

