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
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.entities.EventSearchRequest
import com.exactpro.th2.reportdataprovider.entities.ProviderEventId
import com.exactpro.th2.reportdataprovider.getEventSuspend
import com.exactpro.th2.reportdataprovider.getEventsSuspend
import com.fasterxml.jackson.annotation.JsonIgnore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.time.Instant

data class EventTreeNode(
    val eventId: String,
    val eventName: String,
    val eventType: String,
    val isSuccessful: Boolean,
    val startTimestamp: Instant,
    @JsonIgnore
    val parentEventId: String?,
    val childList: MutableList<EventTreeNode>
) {
    constructor(batchId: StoredTestEventId?, data: StoredTestEvent) : this(
        eventId =  ProviderEventId(batchId, data.id).toString(),
        eventName = data.name,
        eventType = data.type,
        isSuccessful = data.isSuccess,
        startTimestamp = data.startTimestamp,
        parentEventId = data.parentId?.toString(),
        childList = mutableListOf<EventTreeNode>()
    )
}

fun EventTreeNode.addChild(child: EventTreeNode) {
    childList.add(child)
}


suspend fun searchChildrenEvents(
    request: EventSearchRequest,
    cradleManager: CradleManager,
    timeout: Long
): List<EventTreeNode> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {

            val linker = cradleManager.storage.testEventsMessagesLinker

            val filteredList = cradleManager.storage.getEventsSuspend(
                request.timestampFrom,
                request.timestampTo
            )
                .filter {
                    (request.type == null || request.type.contains(it.type))
                            && (request.name == null || request.name.any { item -> it.name.contains(item, true) })
                            && (
                            request.attachedMessageId == null ||
                                    linker.getTestEventIdsByMessageId(StoredMessageId.fromString(request.attachedMessageId))
                                        .contains(it.id)
                            )
                }
                .flatMap {
                    if (it.isBatch) {
                        getDirectBatchedChildren(it.id, request, cradleManager)
                    } else {
                        listOf(EventTreeNode(null, it))
                    }
                }

            val eventTreeMap = mapOf(*filteredList.map { it.eventId to it }.toTypedArray())

            val result = mutableListOf<EventTreeNode>()

            for (event in filteredList) {
                if (event.parentEventId != null && eventTreeMap.containsKey(event.parentEventId))
                    eventTreeMap[event.parentEventId]?.addChild(event)
                else
                    result.add(event)
            }

            result
        }
    }
}


suspend fun getDirectBatchedChildren(
    batchId: StoredTestEventId,
    request: EventSearchRequest,
    cradleManager: CradleManager
): List<EventTreeNode> {
    val linker = cradleManager.storage.testEventsMessagesLinker

    return (cradleManager.storage.getEventSuspend(batchId)?.asBatch()?.testEvents
        ?: throw IllegalArgumentException("unable to get test events of batch $batchId"))
        .filter {
            it.startTimestamp.isAfter(request.timestampFrom)
                    && it.startTimestamp.isBefore(request.timestampTo)
                    && (request.type == null || request.type.contains(it.type))
                    && (request.name == null || request.name.any { item -> it.name.contains(item, true) })
                    && (
                    request.attachedMessageId == null ||
                            linker.getTestEventIdsByMessageId(StoredMessageId.fromString(request.attachedMessageId))
                                .contains(it.id)
                    )
        }
        .map { EventTreeNode(batchId, it) }

}
