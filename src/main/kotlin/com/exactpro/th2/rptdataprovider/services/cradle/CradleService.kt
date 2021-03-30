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


package com.exactpro.th2.rptdataprovider.services.cradle

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import kotlin.coroutines.coroutineContext

class CradleService(configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesAsyncGauge: Metrics = Metrics("get_messages_async", "getMessagesAsync")
        private val getProcessedMessageAsyncGauge: Metrics =
            Metrics("get_processed_message_async", "getProcessedMessageAsync")
        private val getMessageAsyncGauge: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncGauge: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncGauge: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getNearestMessageIdGauge: Metrics = Metrics("get_nearest_message_id", "getNearestMessageId")
        private val getMessageBatchAsyncGauge: Metrics = Metrics("get_message_batch_async", "getMessageBatchAsync")
        private val getTestEventIdsByMessageIdAsyncGauge: Metrics =
            Metrics("get_test_event_ids_by_message_id_async", "getTestEventIdsByMessageIdAsync")
        private val getMessageIdsByTestEventIdAsyncGauge: Metrics =
            Metrics("get_message_ids_by_test_event_id_async", "getMessageIdsByTestEventIdAsync")
        private val getStreamsGauge: Metrics =
            Metrics("get_streams", "getStreams")
    }

    private val cradleManager: CradleManager = configuration.cradleManager

    private val storage = cradleManager.storage
    private val linker = cradleManager.storage.testEventsMessagesLinker


    suspend fun getMessagesSuspend(filter: StoredMessageFilter): Iterable<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessagesAsyncGauge) {
                logTime("getMessages (filter=${filter.convertToString()})") {
                    storage.getMessagesAsync(filter).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logMetrics(getProcessedMessageAsyncGauge) {
                logTime("getProcessedMessage (id=$id)") {
                    storage.getProcessedMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageAsyncGauge) {
                logTime("getMessage (id=$id)") {
                    storage.getMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getEventsSuspend(from: Instant, to: Instant): Iterable<StoredTestEventMetadata> {
        return withContext(coroutineContext) {
            logMetrics(getTestEventsAsyncGauge) {
                logTime("Get events from: $from to: $to") {
                    storage.getTestEventsAsync(from, to).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventsSuspend(
        parentId: StoredTestEventId,
        from: Instant,
        to: Instant
    ): Iterable<StoredTestEventMetadata> {
        return withContext(coroutineContext) {
            logMetrics(getTestEventsAsyncGauge) {
                logTime("Get events parent: $parentId from: $from to: $to") {
                    storage.getTestEventsAsync(parentId, from, to).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
        return withContext(coroutineContext) {
            logMetrics(getTestEventAsyncGauge) {
                logTime("getTestEvent (id=$id)") {
                    storage.getTestEventAsync(id).await()
                }
            }
        }
    }

    suspend fun getFirstMessageIdSuspend(
        timestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation
    ): StoredMessageId? {
        return withContext(Dispatchers.IO) {
            logMetrics(getNearestMessageIdGauge) {
                logTime(("getFirstMessageId (timestamp=$timestamp stream=$stream direction=${direction.label} )")) {
                    storage.getNearestMessageId(stream, direction, timestamp, timelineDirection)
                }
            }
        }
    }

    suspend fun getMessageBatchSuspend(id: StoredMessageId): Collection<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageBatchAsyncGauge) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    storage.getMessageBatchAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
        return withContext(Dispatchers.IO) {
            logMetrics(getTestEventIdsByMessageIdAsyncGauge) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    linker.getTestEventIdsByMessageIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageIdsSuspend(id: StoredTestEventId): Collection<StoredMessageId> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageIdsByTestEventIdAsyncGauge) {
                logTime("getMessageIdsByTestEventId (id=$id)") {
                    linker.getMessageIdsByTestEventIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageStreams(): Collection<String> {
        return withContext(Dispatchers.IO) {
            logMetrics(getStreamsGauge) {
                logTime("getStreams") {
                    storage.streams
                }
            } ?: emptyList()
        }
    }
}
