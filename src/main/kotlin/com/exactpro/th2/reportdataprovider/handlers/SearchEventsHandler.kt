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

package com.exactpro.th2.reportdataprovider.handlers


import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.reportdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.reportdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.reportdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.reportdataprovider.services.CradleService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging

class SearchEventsHandler(private val cradle: CradleService, private val timeout: Long) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    suspend fun searchEvents(request: EventSearchRequest): List<Any> {
        return withContext(Dispatchers.Default) {
            withTimeout(timeout) {

                val baseList = cradle.getEventsSuspend(
                    request.timestampFrom,
                    request.timestampTo
                ).flatMap { metadata ->
                    if (metadata.isBatch) {
                        metadata.batchMetadata?.rootTestEvents
                            ?.map { EventTreeNode(metadata.batchMetadata, it) } ?: listOf()
                    } else {
                        listOf(EventTreeNode(null, metadata))
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
            event.parentEventId = null

            logger.error { "non-root event '${event.id}' has a parent '${event.parentEventId}' which is missing in cradle" }
            event.parentEventId = null
            return
        }

        recursiveParentSearch(parentEvent, result)
    }

    private suspend fun buildEventTree(
        childrenPool: List<EventTreeNode>,
        filteredList: List<EventTreeNode>
    ): List<EventTreeNode> {

        val eventTreeMap =
            filteredList.associateBy({ it.id }, { it }) as MutableMap

        val childrenPoolMap =
            childrenPool.associateBy({ it.id }, { it }) as MutableMap

        // add all parents not included in the filter
        for (event in filteredList) {
            if (event.parentEventId?.let { !eventTreeMap.containsKey(it) } == true) {
                recursiveParentSearch(event, eventTreeMap)
            }
        }

        eventTreeMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it]?.addChild(event) }
        }

        // for each element (except for the root ones) indicate its parent among the filtered ones
        childrenPoolMap.values.forEach { event ->
            event.parentEventId?.also { eventTreeMap[it]?.addChild(event) }
        }

        // take only root elements
        return eventTreeMap.values.filter { it.parentEventId == null }
    }
}
