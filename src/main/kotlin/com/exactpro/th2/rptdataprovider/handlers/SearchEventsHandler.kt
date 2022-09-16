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
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.internal.IntermediateEvent
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedEventInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.events.SearchInterval
import com.exactpro.th2.rptdataprovider.handlers.events.TimeIntervalGenerator
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleIteratorWrapper
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.time.Instant
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


    private suspend fun keepAlive(writer: StreamWriter, info: LastScannedObjectInfo, counter: AtomicLong) {
        while (coroutineContext.isActive) {
            writer.write(info, counter)
            delay(keepAliveTimeout)
        }
    }


    private fun getDirection(searchDirection: TimeRelation): Order {
        return if (searchDirection == AFTER) {
            Order.DIRECT
        } else {
            Order.REVERSE
        }
    }


    private suspend fun getTestEvents(searchInterval: SearchInterval, order: Order): Iterable<StoredTestEventWrapper> {
        return if (searchInterval.resumeId != null) {
            if (order == Order.DIRECT) {
                cradle.getEventsSuspend(searchInterval.resumeId, searchInterval.endInterval, order)
            } else {
                cradle.getEventsSuspend(searchInterval.startInterval, searchInterval.resumeId, order)
            }
        } else {
            cradle.getEventsSuspend(searchInterval.startInterval, searchInterval.endInterval, order)
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
                    }.orEmpty()
                } else {
                    getTestEvents(searchInterval, order)
                        .filter { it.parentId == parentEvent.eventId }
                }
            } else {
                getTestEvents(searchInterval, order)
            }
        }
    }


    private fun isBefore(event: BaseEventEntity, instant: Instant, searchDirection: TimeRelation): Boolean {
        return if (searchDirection == AFTER) {
            event.startTimestamp.isBefore(instant)
        } else {
            event.startTimestamp.isAfter(instant)
        }
    }


    private fun isAfterOrEqual(event: BaseEventEntity, timestamp: Instant?, direction: TimeRelation): Boolean {
        if (timestamp == null) return false

        return if (direction == AFTER) {
            event.startTimestamp.isAfterOrEqual(timestamp)
        } else {
            event.startTimestamp.isBeforeOrEqual(timestamp)
        }
    }


    private fun createIntermediateEvents(
        wrapper: StoredTestEventWrapper,
        request: SseEventSearchRequest
    ): List<IntermediateEvent> {
        val parentEventId = request.parentEvent?.eventId

        return if (wrapper.isBatch) {
            val batch = wrapper.asBatch()
            batch.tryToGetTestEvents(parentEventId).map { event ->
                IntermediateEvent(event, eventProducer.fromStoredEvent(event, batch))
            }
        } else {
            val single = wrapper.asSingle()
            if (parentEventId?.let { it == single.parentId } != false) {
                listOf(IntermediateEvent(single, eventProducer.fromStoredEvent(single, null)))
            } else {
                emptyList()
            }
        }
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
                val eventsCollection = CradleIteratorWrapper(getEventsSuspend(request, searchInterval).iterator())

                for (event in eventsCollection)
                    emit(event)
            }
                .map { wrappers ->
                    async(parentContext) {
                        val baseEventEntities = createIntermediateEvents(wrappers, request)

                        val filteredEventEntities = orderedByDirection(baseEventEntities, request.searchDirection)
                            .dropWhile {
                                isBefore(it.baseEventEntity, searchInterval.startRequest, request.searchDirection)
                            }
                            .dropLastWhile {
                                isAfterOrEqual(it.baseEventEntity, searchInterval.endRequest, request.searchDirection)
                            }

                        eventProducer.fromEventsProcessed(filteredEventEntities, request).also {
                            parentContext.ensureActive()
                        }
                    }
                }
                .buffer(BUFFERED)
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

            val resumeEvent = eventWrapper?.let { getEvent(it, resumeId) }
            val parentEventCounter = ParentEventCounter(request.limitForParent)

            val timeIntervals = TimeIntervalGenerator(request, resumeId, eventWrapper)

            flow {
                for (timestamp in timeIntervals) {

                    lastScannedObject.update(timestamp.startInterval)

                    getEventFlow(
                        request, timestamp, currentCoroutineContext()
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
                        eventFlow
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
                    currentCoroutineContext().cancelChildren()
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
