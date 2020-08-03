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
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.*
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
import com.exactpro.th2.reportdataprovider.entities.Message
import com.exactpro.th2.reportdataprovider.entities.MessageSearchRequest
import com.exactpro.th2.reportdataprovider.entities.TimelineDirection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Instant

private val logger = KotlinLogging.logger { }

private suspend fun pullMore(
    startId: StoredMessageId,
    limit: Int,
    timelineDirection: TimelineDirection,
    manager: CradleManager
): List<StoredMessage> {
    logger.debug { "pulling more messages (id=$startId limit=$limit direction=$timelineDirection)" }

    return manager.storage.getMessagesSuspend(
        StoredMessageFilterBuilder()
            .let {
                if (timelineDirection == TimelineDirection.NEXT) {
                    it.next(startId, limit)
                } else {
                    it.previous(startId, limit)
                }
            }
            .build()
    )

        .let { list ->
            if (timelineDirection == TimelineDirection.NEXT) {
                list.sortedBy { it.timestamp }
            } else {
                list.sortedByDescending { it.timestamp }
            }
        }
        .toList()
}

private suspend fun getNearestMessageId(
    timestamp: Instant,
    stream: String,
    direction: Direction,
    timelineDirection: TimelineDirection,
    manager: CradleManager
): StoredMessageId {
    logger.debug { "getting nearest message id (timestamp=$timestamp stream=$stream(${direction.label}) direction=$timelineDirection)" }

    var currentId = manager.storage.getFirstMessageIdSuspend(
        Instant.ofEpochSecond(timestamp.epochSecond + if (timelineDirection == TimelineDirection.PREVIOUS) 1 else 0),
        stream,
        direction
    )!!

    return flow {
        emit(manager.storage.getMessageSuspend(currentId)!!)

        do {
            val data = pullMore(currentId, 1000, timelineDirection, manager)
            var hasData = false

            for (item in data) {
                emit(item)
                hasData = true
            }

            currentId = data.last().id
        } while (hasData)
    }
        .first {
            if (timelineDirection == TimelineDirection.NEXT) {
                it.timestamp == timestamp || it.timestamp.isAfter(timestamp)
            } else {
                it.timestamp == timestamp || it.timestamp.isBefore(timestamp)
            }
        }
        .id
}

private suspend fun pullMoreMerged(
    startTimestamp: Instant,
    streams: List<String>,
    timelineDirection: TimelineDirection,
    perStreamLimit: Int,
    manager: CradleManager
): List<StoredMessage> {
    logger.debug { "pulling more messages (timestamp=$startTimestamp streams=$streams direction=$timelineDirection perStreamLimit=$perStreamLimit)" }

    return streams
        .distinct()
        .flatMap { listOf(it to Direction.FIRST, it to Direction.SECOND) }
        .flatMap { pair ->
            pullMore(
                if (timelineDirection == TimelineDirection.NEXT) {
                    getNearestMessageId(
                        startTimestamp,
                        pair.first,
                        pair.second,
                        TimelineDirection.NEXT,
                        manager
                    )
                } else {
                    getNearestMessageId(
                        startTimestamp,
                        pair.first,
                        pair.second,
                        TimelineDirection.PREVIOUS,
                        manager
                    )
                },

                perStreamLimit,
                timelineDirection,
                manager
            )
        }
        .let { list ->
            if (timelineDirection == TimelineDirection.NEXT) {
                list.sortedBy { it.timestamp }
            } else {
                list.sortedByDescending { it.timestamp }
            }
        }
}

suspend fun searchMessages(
    request: MessageSearchRequest,
    manager: CradleManager,
    messageCache: MessageCacheManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            val linker = manager.storage.testEventsMessagesLinker

            flow {
                do {
                    val data = pullMoreMerged(
                        request.messageId?.let {
                            manager.storage.getMessageSuspend(StoredMessageId.fromString(it))?.timestamp
                        } ?: (
                                if (request.timelineDirection == TimelineDirection.NEXT) {
                                    request.timestampFrom
                                } else {
                                    request.timestampTo
                                }

                                ) ?: Instant.now(),

                        request.stream ?: emptyList(),
                        request.timelineDirection,
                        request.limit,
                        manager
                    )

                    for (item in data) {
                        emit(item)
                    }
                } while (data.isNotEmpty())
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
                .filter { it.second }
                .takeWhile {
                    it.first.timestamp.let { timestamp ->
                        if (request.timelineDirection == TimelineDirection.NEXT) {
                            request.timestampTo == null
                                    || timestamp.isBefore(request.timestampTo) || timestamp == request.timestampTo
                        } else {
                            request.timestampFrom == null
                                    || timestamp.isAfter(request.timestampTo) || timestamp == request.timestampTo
                        }
                    }
                }
                .take(request.limit)
                .toList()
                .distinctBy { it.first.id }
                .filterNot { it.first.id.toString() == request.messageId }
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
        }
    }
}
