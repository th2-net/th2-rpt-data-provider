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
import com.exactpro.th2.rptdataprovider.asyncClose
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.sse.EventType
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.eventWrite
import com.exactpro.th2.rptdataprovider.min
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.io.Writer
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

class SearchEventsHandler(private val cradle: CradleService) {
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


    private suspend fun getEventTreeNodeFlow(
        parentEvent: String?,
        timestampFrom: Instant,
        timestampTo: Instant,
        parentContext: CoroutineContext,
        bufferSize: Int
    ): Flow<Deferred<List<EventTreeNode>>> {
        return coroutineScope {
            flow {
                for (event in getEventsSuspend(parentEvent, timestampFrom, timestampTo))
                    emit(event)
            }.cancellable()
                .map { metadata ->
                    async(parentContext) {
                        if (metadata.isBatch) {
                            metadata.batchMetadata?.testEvents
                                ?.map { EventTreeNode(metadata.batchMetadata, it) }

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

    suspend fun searchEvents(request: EventSearchRequest): List<Any> {
        return coroutineScope {
            val baseList = getEventTreeNodeFlow(
                request.parentEvent, request.timestampFrom,
                request.timestampTo, coroutineContext,
                UNLIMITED
            ).map {
                coroutineContext.ensureActive()
                it.await()
            }
                .toList()
                .flatten()

            val filteredList =
                baseList.filter { isEventMatched(it, request.type, request.name, request.attachedMessageId) }

            if (request.flat)
                filteredList.map { it.eventId }
            else
                buildEventTree(baseList, filteredList)
        }
    }

    private suspend fun changeOfDayProcessing(from: Instant, to: Instant): Iterable<Pair<Instant, Instant>> {
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
    ): Iterable<Pair<Instant, Instant>> {
        return mutableListOf<Pair<Instant, Instant>>().apply {
            val timePair =
                if (request.searchDirection == TimeRelation.AFTER) {
                    Pair(request.startTimestamp, request.startTimestamp.plusMillis(request.timeLimit))
                } else {
                    Pair(request.startTimestamp.minusMillis(request.timeLimit), request.startTimestamp)
                }

            var timestamp = timePair.first
            while (timestamp.isBefore(timePair.second)) {
                addAll(
                    changeOfDayProcessing(
                        timestamp,
                        timestamp.plusSeconds(sseEventSearchStep)
                            .min(timePair.second).also { timestamp = it }
                    )
                )
            }
            if (request.searchDirection == TimeRelation.BEFORE) reverse()
        }
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    suspend fun searchEventsSse(
        request: SseEventSearchRequest,
        jacksonMapper: ObjectMapper,
        sseEventSearchStep: Long,
        writer: Writer
    ) {
        withContext(coroutineContext) {
            val timeIntervals = getTimeIntervals(request, sseEventSearchStep)
            flow {
                for (timestamp in timeIntervals) {
                    getEventTreeNodeFlow(
                        request.parentEvent, timestamp.first,
                        timestamp.second, coroutineContext,
                        BUFFERED
                    ).collect { emit(it) }
                }
            }
                .map { it.await() }
                .flatMapMerge { it.asFlow() }
                .filter { isEventMatched(it, request.type, request.name, request.attachedMessageId) }
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
        result: MutableMap<StoredTestEventId, EventTreeNode>
    ) {

        result.computeIfAbsent(event.id.eventId) { event }

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

        val eventTreeMap: MutableMap<StoredTestEventId, EventTreeNode> =
            filteredList.associateBy({ it.id.eventId }, { it }) as MutableMap

        val unfilteredEventMap: MutableMap<StoredTestEventId, EventTreeNode> =
            unfilteredList.associateBy({ it.id.eventId }, { it }) as MutableMap

        // add all parents not included in the filter
        for (event in filteredList) {
            if (event.parentEventId?.let { !eventTreeMap.containsKey(it.eventId) } == true) {
                recursiveParentSearch(event, eventTreeMap)
            }
        }

        // TODO("FIX ME")
        // for each element in unfiltered events (except for the root ones) indicate its parent
        // building tree from unfiltered elements
        unfilteredEventMap.values.forEach { event ->
            event.parentEventId?.also { unfilteredEventMap[it.eventId]?.addChild(event) }
        }

        // building a tree from filtered elements. In recursiveParentSearch we have added
        // new items to eventTreeMap and now we need to insert them into the tree
        eventTreeMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it.eventId]?.addChild(event) }
        }

        // New elements found in the previous step could be absent among the
        // unfiltered elements, but have children among them
        unfilteredEventMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it.eventId]?.addChild(event) }
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
