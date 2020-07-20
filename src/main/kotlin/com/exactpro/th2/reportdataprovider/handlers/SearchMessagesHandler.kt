/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.*
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
import com.exactpro.th2.reportdataprovider.entities.Message
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

suspend fun searchMessages(
    request: MessageSearchRequest,
    manager: CradleManager,
    messageCache: MessageCacheManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            var message = request.messageId?.let { manager.storage.getMessageSuspend(StoredMessageId.fromString(it)) }
            var limitReached = false

            suspend fun pullMore(): List<StoredMessage> {
                if (limitReached) {
                    return emptyList()
                }

                val timestampFrom = message.let {
                    if (it != null && request.timelineDirection == TimelineDirection.NEXT) {
                        it.timestamp
                    } else {
                        request.timestampFrom
                    }
                }

                val timestampTo = message.let {
                    if (it != null && request.timelineDirection == TimelineDirection.PREVIOUS) {
                        it.timestamp
                    } else {
                        request.timestampTo
                    }
                }

                return request.stream
                    ?.distinct()
                    ?.flatMap { streamName ->
                        manager.storage.getMessagesSuspend(
                            StoredMessageFilterBuilder()
                                .streamName().isEqualTo(streamName)
                                .let {
                                    if (timestampFrom != null)
                                        it.timestampFrom().isGreaterThanOrEqualTo(timestampFrom) else it
                                }
                                .let {
                                    if (timestampTo != null)
                                        it.timestampTo().isLessThanOrEqualTo(timestampTo) else it
                                }
                                .let {
                                    it.limit(request.limit)
                                }
                                .build()
                        )
                    }

                    ?.let { flow ->
                        if (request.timelineDirection == TimelineDirection.NEXT) {
                            flow.sortedBy { it.timestamp }
                        } else {
                            flow.sortedByDescending { it.timestamp }
                        }
                    }

                    ?.also {
                        try {
                            message = it.last()
                        } catch (e: NoSuchElementException) {
                            limitReached = true
                        }
                    }

                    ?: emptyList()
            }

            val linker = manager.storage.testEventsMessagesLinker

            flow {
                var data = pullMore()

                while (data.isNotEmpty()) {
                    data.forEach { emit(it) }
                    data = pullMore()
                }
            }
                .map { messageToFilter ->
                    async {
                        messageToFilter to (
                                (request.attachedEventId?.let {
                                    linker.getEventIdsSuspend(messageToFilter.id)
                                        .contains(StoredTestEventId(it))
                                } ?: true)

                                        && (request.messageType?.contains(
                                    manager.storage.getProcessedMessageSuspend(messageToFilter.id)?.getMessageType()
                                        ?: "unknown"
                                ) ?: true)
                                )
                    }
                }
                .map { it.await() }
                .filter { it.second && (!(message?.id?.equals(it.first.id) ?: false)) }
                .take(request.limit)
                .map {
                    async {
                        val event = it.first

                        if (request.idsOnly) {
                            event.id.toString()
                        } else {
                            messageCache.get(event.id.toString())
                                ?: Message(
                                    manager.storage.getProcessedMessageSuspend(
                                        event.id
                                    ), event
                                )
                                    .let { message ->
                                        messageCache.put(event.id.toString(), message)
                                        message
                                    }
                        }
                    }
                }
                .map { it.await() }
                .toList()
        }
    }
}
