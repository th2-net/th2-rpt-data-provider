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
import com.exactpro.th2.reportdataprovider.entities.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.getEventIdsSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

suspend fun getRootEvents(
    request: EventSearchRequest,
    manager: CradleManager,
    eventCache: EventCacheManager,
    timeout: Long
): List<Any> {
    val linker = manager.storage.testEventsMessagesLinker

    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            manager.storage.rootTestEvents.asFlow()
                .map { event ->
                    async {
                        event to (
                                (request.attachedMessageId?.let {
                                    linker.getEventIdsSuspend(StoredMessageId.fromString(it)).contains(event.id)
                                } ?: true)

                                        && (request.name?.any { event.name.toLowerCase().contains(it.toLowerCase()) }
                                    ?: true)

                                        && (request.type?.any { event.type.toLowerCase().contains(it.toLowerCase()) }
                                    ?: true)

                                        && (request.timestampFrom
                                    ?.let {
                                        event.endTimestamp?.isAfter(it) ?: (event.startTimestamp?.isAfter(it) ?: false)
                                    } ?: true)

                                        && (request.timestampTo
                                    ?.let {
                                        event.startTimestamp?.isBefore(it) ?: false }
                                    ?: true)
                                )
                    }
                }
                .map { it.await() }
                .filter { it.second }
                .toList()
                .sortedByDescending { it.first.startTimestamp }
                .map {

                    async {
                        val id = it.first.id.toString()

                        if (request.idsOnly) {
                            id
                        } else {
                            eventCache.getOrPut(id)
                        }
                    }
                }
                .mapNotNull { it.await() }
        }
    }
}
