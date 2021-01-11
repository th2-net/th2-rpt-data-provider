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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventBatchMetadata
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.RequestType
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.sse.EventType
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.eventWrite
import com.exactpro.th2.rptdataprovider.maxInstant
import com.exactpro.th2.rptdataprovider.minInstant
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.cradle.databaseRequestRetry
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.*
import io.ktor.utils.io.errors.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import mu.KotlinLogging
import java.io.Writer
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.coroutines.CoroutineContext

class SearchEventsHandler(private val cradle: CradleService, private val dbRetryDelay: Long) {
    companion object {
        private val logger = KotlinLogging.logger { }
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


    @ExperimentalCoroutinesApi
    private suspend fun getEventTreeNodeFlow(
        parentEvent: String?,
        timestampFrom: Instant,
        timestampTo: Instant,
        parentContext: CoroutineContext,
        bufferSize: Int,
        requestType: RequestType
    ): Flow<Deferred<List<EventTreeNode>>> {
        return coroutineScope {
            flow {
                val eventsCollection = if (requestType == RequestType.SSE)
                    databaseRequestRetry(dbRetryDelay) { getEventsSuspend(parentEvent, timestampFrom, timestampTo) }
                else
                    getEventsSuspend(parentEvent, timestampFrom, timestampTo)

                for (event in eventsCollection)
                    emit(event)
            }
                .map { metadata ->
                    async(parentContext) {
                        if (metadata.isBatch) {
                            try {
                                metadata.batchMetadata
                            } catch (e: IOException) {
                                null
                            }
                                ?.testEvents?.map { EventTreeNode(metadata.batchMetadata, it) }

                                ?: getDirectBatchedChildren(metadata.id, timestampFrom, timestampTo)
                        } else {
                            listOf(EventTreeNode(null, metadata))
                        }
                    }.also { parentContext.ensureActive() }
                }
                .buffer(bufferSize)
        }
    }

    private suspend fun isEventMatched(
        event: EventTreeNode,
        type: List<String>?,
        name: List<String>?,
        attachedMessageId: String?
    ): Boolean {
        return (type == null || type.any { item ->
            event.eventType.toLowerCase().contains(item.toLowerCase())
        }) && (name == null || name.any { item ->
            event.eventName.toLowerCase().contains(item.toLowerCase())
        }) && (attachedMessageId == null ||
                cradle.getEventIdsSuspend(StoredMessageId.fromString(attachedMessageId))
                    .contains(StoredTestEventId(event.eventId)))
    }

    @ExperimentalCoroutinesApi
    suspend fun searchEvents(request: EventSearchRequest): List<Any> {
        return coroutineScope {
            val baseList = getEventTreeNodeFlow(
                request.parentEvent, request.timestampFrom,
                request.timestampTo, coroutineContext,
                UNLIMITED,
                RequestType.REST
            ).map {
                coroutineContext.ensureActive()
                it.await()
            }.toList().flatten()

            val filteredList =
                baseList.filter { isEventMatched(it, request.type, request.name, request.attachedMessageId) }

            if (request.flat)
                filteredList.map { it.eventId }
            else
                buildEventTree(baseList, filteredList)
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

    private suspend fun getTimeIntervals(
        request: SseEventSearchRequest,
        sseEventSearchStep: Long
    ): Iterator<Pair<Instant, Instant>> {
        return sequence {
            var timestamp = request.startTimestamp
            while (timestamp.isAfter(Instant.MIN) && timestamp.isBefore(Instant.MAX)) {
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
        }.iterator()
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(
        request: SseEventSearchRequest,
        jacksonMapper: ObjectMapper,
        sseEventSearchStep: Long,
        writer: Writer
    ) {
        coroutineScope {
            val timeIntervals = getTimeIntervals(request, sseEventSearchStep)
            flow {
                for (timestamp in timeIntervals) {
                    getEventTreeNodeFlow(
                        request.parentEvent, timestamp.first,
                        timestamp.second, coroutineContext,
                        BUFFERED, RequestType.SSE
                    ).collect { emit(it) }
                }
            }
                .map { it.await() }
                .flatMapMerge { it.asFlow() }
                .filter { request.filterPredicate.apply(it) }
                .take(request.resultCountLimit)
                .onCompletion {
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()
                    writer.eventWrite(SseEvent(jacksonMapper.asStringSuspend(it), EventType.EVENT, it.eventId))
                }
        }
    }

    private suspend fun recursiveParentSearch(
        event: EventTreeNode,
        result: MutableMap<ProviderEventId, EventTreeNode>
    ) {

        result.computeIfAbsent(event.id) { event }

        val parentEventId = event.parentEventId?.eventId ?: return
        val batch = event.batch

        val parentEvent =
            batch?.getTestEvent(parentEventId)?.let { EventTreeNode(batch, it, false) }
                ?: cradle.getEventSuspend(parentEventId)?.asSingle()?.let {
                    EventTreeNode(null, StoredTestEventMetadata(it), false)
                }

        if (parentEvent == null) {
            logger.error { "non-root event '${event.id}' has a parent '${event.parentEventId}' which is missing in cradle" }
            event.parentEventId = null
            return
        }

        recursiveParentSearch(parentEvent, result)
    }

    private suspend fun buildEventTree(
        unfilteredList: List<EventTreeNode>,
        filteredList: List<EventTreeNode>
    ): List<EventTreeNode> {

        val eventTreeMap: MutableMap<ProviderEventId, EventTreeNode> =
            filteredList.associateBy({ it.id }, { it }) as MutableMap

        val unfilteredEventMap: MutableMap<ProviderEventId, EventTreeNode> =
            unfilteredList.associateBy({ it.id }, { it }) as MutableMap

        // add all parents not included in the filter
        for (event in filteredList) {
            if (event.parentEventId?.let { !eventTreeMap.containsKey(it) } == true) {
                recursiveParentSearch(event, eventTreeMap)
            }
        }

        // TODO("FIX ME")
        // for each element in unfiltered events (except for the root ones) indicate its parent
        // building tree from unfiltered elements
        unfilteredEventMap.values.forEach { event ->
            event.parentEventId?.also { unfilteredEventMap[it]?.addChild(event) }
        }

        // building a tree from filtered elements. In recursiveParentSearch we have added
        // new items to eventTreeMap and now we need to insert them into the tree
        eventTreeMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it]?.addChild(event) }
        }

        // New elements found in the previous step could be absent among the
        // unfiltered elements, but have children among them
        unfilteredEventMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it]?.addChild(event) }
        }

        // take only root elements
        return eventTreeMap.values.filter { it.parentEventId == null }
    }

    // this is a fallback that should be deprecated after migration to cradle 1.6
    private suspend fun getDirectBatchedChildren(
        batchId: StoredTestEventId,
        timestampFrom: Instant,
        timestampTo: Instant
    ): List<EventTreeNode> {

        val batch = cradle.getEventSuspend(batchId)?.asBatch()

        return (batch?.testEvents
            ?: throw CradleEventNotFoundException("unable to get test events of batch '$batchId'"))
            .filter { it.startTimestamp.isAfter(timestampFrom) && it.startTimestamp.isBefore(timestampTo) }
            .map {
                val batchMetadata = StoredTestEventBatchMetadata(batch)

                EventTreeNode(batchMetadata, BatchedStoredTestEventMetadata(it, batchMetadata))
            }
    }
}
