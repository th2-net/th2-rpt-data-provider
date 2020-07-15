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

import com.exactpro.th2.reportdataprovider.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.nio.file.Paths

suspend fun searchChildrenEvents(
    request: EventSearchRequest,
    pathString: String,
    eventCache: EventCacheManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            (eventCache.getOrPut(pathString)?.childrenIds?.asFlow() ?: listOf<String>().asFlow())
                .map { id ->

                    async {
                        val event = eventCache.getOrPut(Paths.get(pathString, id).toString())!!

                        event to (
                                (request.attachedMessageId?.let {
                                    event.attachedMessageIds?.contains(request.attachedMessageId) ?: false
                                } ?: true)

                                        && (request.name
                                    ?.any { event.eventName.toLowerCase().contains(it.toLowerCase()) }
                                    ?: true)

                                        && (request.type
                                    ?.any { event.eventType?.toLowerCase()?.contains(it.toLowerCase()) ?: false }
                                    ?: true)

                                        && (request.timestampFrom
                                    ?.let { event.endTimestamp?.isAfter(it) ?: (event.startTimestamp.isAfter(it)) }
                                    ?: true)

                                        && (request.timestampTo?.let { event.startTimestamp.isBefore(it) }
                                    ?: true)
                                )
                    }
                }
                .map { it.await() }
                .filter { it.second }
                .toList()
                .sortedByDescending { it.first.startTimestamp }
                .map {
                    if (request.idsOnly) {
                        it.first.eventId
                    } else {
                        it.first
                    }
                }
        }
    }
}
