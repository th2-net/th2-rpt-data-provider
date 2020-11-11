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


import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventBatchMetadata
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.isActive
import mu.KotlinLogging
import java.time.Instant

class SearchEventsHandler(private val cradle: CradleService) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    suspend fun searchEvents(request: EventSearchRequest): List<Any> {
        return coroutineScope {

            val baseList = cradle.getEventsSuspend(
                request.timestampFrom,
                request.timestampTo
            ).asFlow()
                .map { metadata ->
                    async {
                        if (metadata.isBatch) {
                            metadata.batchMetadata?.testEvents
                                ?.map { EventTreeNode(metadata.batchMetadata, it) }

                                ?: getDirectBatchedChildren(metadata.id, request.timestampFrom, request.timestampTo)
                        } else {
                            listOf(EventTreeNode(null, metadata))
                        }
                    }.also { if (!coroutineContext.isActive) throw CancellationException() }
                }
                .toList()
                .flatMap {
                    if (coroutineContext.isActive) {
                        it.await()
                    } else {
                        throw CancellationException()
                    }
                }


            val filteredList = baseList
                .filter {
                    (request.type == null || request.type.any { item ->
                        it.eventType.toLowerCase().contains(item.toLowerCase())
                    })

                            && (request.name == null || request.name.any { item ->
                        it.eventName.toLowerCase().contains(item.toLowerCase())
                    })

                            && (request.attachedMessageId == null ||
                            cradle.getEventIdsSuspend(StoredMessageId.fromString(request.attachedMessageId))
                                .contains(StoredTestEventId(it.eventId)))
                }

            if (request.flat)
                filteredList.map { it.eventId }
            else
                buildEventTree(baseList, filteredList)
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
            ?: throw IllegalArgumentException("unable to get test events of batch '$batchId'"))
            .filter { it.startTimestamp.isAfter(timestampFrom) && it.startTimestamp.isBefore(timestampTo) }
            .map {
                val batchMetadata = StoredTestEventBatchMetadata(batch)

                EventTreeNode(batchMetadata, BatchedStoredTestEventMetadata(it, batchMetadata))
            }
    }
}
