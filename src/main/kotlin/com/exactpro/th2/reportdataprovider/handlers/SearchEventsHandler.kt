/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider.handlers


import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.entities.EventSearchRequest
import com.exactpro.th2.reportdataprovider.entities.ProviderEventId
import com.exactpro.th2.reportdataprovider.getEventSuspend
import com.exactpro.th2.reportdataprovider.getEventsSuspend
import com.fasterxml.jackson.annotation.JsonIgnore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Instant


private val logger = KotlinLogging.logger { }

@Suppress("MemberVisibilityCanBePrivate")
class EventTreeNode(
    val eventId: String,
    val eventName: String,
    val eventType: String,
    val successful: Boolean,
    val startTimestamp: Instant,
    @JsonIgnore
    var parentEventId: String?,
    @JsonIgnore
    var cleanEventId: String,
    val childList: MutableSet<EventTreeNode>,
    var filtered: Boolean,
    @JsonIgnore
    var batch: StoredTestEventBatch?
) {
    constructor(batchId: StoredTestEventId?, batch: StoredTestEventBatch?, data: StoredTestEvent) : this(
        eventId = ProviderEventId(batchId, data.id).toString(),
        cleanEventId = data.id.toString(),
        eventName = data.name ?: "",
        eventType = data.type ?: "",
        successful = data.isSuccess,
        startTimestamp = data.startTimestamp,
        parentEventId = data.parentId?.toString(),
        childList = mutableSetOf<EventTreeNode>(),
        filtered = true,
        batch = batch
    )

    constructor(batchId: StoredTestEventId?, batch: StoredTestEventBatch?, data: StoredTestEventWithContent) : this(
        eventId = ProviderEventId(batchId, data.id).toString(),
        cleanEventId = data.id.toString(),
        eventName = data.name ?: "",
        eventType = data.type ?: "",
        successful = data.isSuccess,
        startTimestamp = data.startTimestamp,
        parentEventId = data.parentId?.toString(),
        childList = mutableSetOf<EventTreeNode>(),
        filtered = true,
        batch = batch
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventTreeNode

        if (cleanEventId != other.cleanEventId) return false

        return true
    }

    override fun hashCode(): Int {
        return cleanEventId.hashCode()
    }

    override fun toString(): String {
        return "EventTreeNode(eventId='$eventId', eventName='$eventName', eventType='$eventType', isSuccessful=$successful, startTimestamp=$startTimestamp, parentEventId=$parentEventId, cleanEventId=$cleanEventId, childList=$childList, filtered=$filtered, batch=$batch)"
    }

}

fun EventTreeNode.addChild(child: EventTreeNode) {
    childList.add(child)
}


suspend fun searchEvents(
    request: EventSearchRequest,
    cradleManager: CradleManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {

            val linker = cradleManager.storage.testEventsMessagesLinker

            val baseList = cradleManager.storage.getEventsSuspend(
                request.timestampFrom,
                request.timestampTo
            ).flatMap {
                if (it.isBatch) {
                    getDirectBatchedChildren(it.id, request.timestampFrom, request.timestampTo, cradleManager)
                } else {
                    listOf(EventTreeNode(null, null, it))
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
                            linker.getTestEventIdsByMessageId(StoredMessageId.fromString(request.attachedMessageId))
                                .contains(StoredTestEventId(it.eventId)))
                }

            if (request.flat)
                filteredList.map { it.eventId }
            else
                buildEventTree(baseList, filteredList, cradleManager)
        }
    }
}

suspend fun recursiveParentSearch(
    event: EventTreeNode,
    result: MutableMap<String, EventTreeNode>,
    cradleManager: CradleManager
) {

    if (!result.containsKey(event.cleanEventId))
        result[event.cleanEventId] = event

    if (event.parentEventId == null) { //element is root
        return
    }

    val parsedId = ProviderEventId(event.parentEventId!!)
    val batch = event.batch
    val parentEvent =
        batch?.getTestEvent(parsedId.eventId) ?: cradleManager.storage.getEventSuspend(parsedId.eventId)?.asSingle()

    if (parentEvent != null) {
        val parent = EventTreeNode(batch?.id, batch, parentEvent).apply {
            filtered = false
        }

        recursiveParentSearch(parent, result, cradleManager)
    } else {
        logger.error { "${parsedId.eventId} is not a valid id" }
        event.parentEventId = null
    }
}

suspend fun buildEventTree(
    childrenPool: List<EventTreeNode>,
    filteredList: List<EventTreeNode>,
    cradleManager: CradleManager
): List<EventTreeNode> {
    val eventTreeMap =
        filteredList.associateBy({ it.cleanEventId }, { it }) as MutableMap

    val childrenPoolMap =
        childrenPool.associateBy({ it.cleanEventId }, { it }) as MutableMap

    // add all parents not included in the filter
    for (event in filteredList) {
        if (event.parentEventId != null && !eventTreeMap.containsKey(event.parentEventId!!))
            recursiveParentSearch(event, eventTreeMap, cradleManager)
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

suspend fun getDirectBatchedChildren(
    batchId: StoredTestEventId,
    timestampFrom: Instant,
    timestampTo: Instant,
    cradleManager: CradleManager
): List<EventTreeNode> {
    val batch = cradleManager.storage.getEventSuspend(batchId)?.asBatch()
    return (batch?.testEvents
        ?: throw IllegalArgumentException("unable to get test events of batch $batchId"))
        .filter { it.startTimestamp.isAfter(timestampFrom) && it.startTimestamp.isBefore(timestampTo) }
        .map { EventTreeNode(batchId, batch, it) }

}
