/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.entities.EventSearchRequest
import com.exactpro.th2.reportdataprovider.getEventSuspend
import com.exactpro.th2.reportdataprovider.getEventsSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

suspend fun searchChildrenEvents(
    request: EventSearchRequest,
    id: String,
    eventCache: EventCacheManager,
    cradleManager: CradleManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            val parent = eventCache.getOrPut(id)
            val linker = cradleManager.storage.testEventsMessagesLinker

            val parentId = StoredTestEventId(parent.eventId)

            if (parent.isBatched) {
                val events =
                    cradleManager.storage.getEventSuspend(StoredTestEventId(parent.batchId))?.asBatch()?.testEvents
                        ?: throw IllegalArgumentException("unable to get test events of batch ${parent.batchId}")

                events
                    .filter {
                        it.parentId == parentId
                                && it.startTimestamp.isAfter(request.timestampFrom)
                                && it.startTimestamp.isBefore(request.timestampTo)
                                && (request.type == null || request.type.contains(it.type))
                                && (request.name == null || request.name.any { item -> it.name.contains(item, true) })
                                && (
                                request.attachedMessageId == null ||
                                        linker.getTestEventIdsByMessageId(StoredMessageId.fromString(request.attachedMessageId))
                                            .contains(it.id)
                                )
                    }
                    .map { it.id }
            } else {
                cradleManager.storage.getEventsSuspend(
                    parentId,
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
                    .map { it.id }
            }
        }
    }
}
