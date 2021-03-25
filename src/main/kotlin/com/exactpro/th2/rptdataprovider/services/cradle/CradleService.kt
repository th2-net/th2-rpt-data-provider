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
import com.exactpro.th2.rptdataprovider.convertToString
import com.exactpro.th2.rptdataprovider.createGauge
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import io.prometheus.client.Gauge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import kotlin.coroutines.coroutineContext

class CradleService(configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesAsync: Gauge = createGauge("get_messages_async", "getMessagesAsync")
        private val getProcessedMessageAsync: Gauge =
            createGauge("get_processed_message_async", "getProcessedMessageAsync")
        private val getMessageAsync: Gauge = createGauge("get_message_async", "getMessageAsync")
        private val getTestEventsAsync: Gauge = createGauge("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsync: Gauge = createGauge("get_test_event_async", "getTestEventAsync")
        private val getNearestMessageId: Gauge = createGauge("get_nearest_message_id", "getNearestMessageId")
        private val getMessageBatchAsync: Gauge = createGauge("get_message_batch_async", "getMessageBatchAsync")
        private val getTestEventIdsByMessageIdAsync: Gauge =
            createGauge("get_test_event_ids_by_message_id_async", "getTestEventIdsByMessageIdAsync")
        private val getMessageIdsByTestEventIdAsync: Gauge =
            createGauge("get_message_ids_by_test_event_id_async", "getMessageIdsByTestEventIdAsync")
        private val getStreams: Gauge =
            createGauge("get_streams", "getStreams")
    }

    private val cradleManager: CradleManager = configuration.cradleManager

    private val storage = cradleManager.storage
    private val linker = cradleManager.storage.testEventsMessagesLinker


    suspend fun getMessagesSuspend(filter: StoredMessageFilter): Iterable<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessagesAsync) {
                logTime("getMessages (filter=${filter.convertToString()})") {
                    storage.getMessagesAsync(filter).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logMetrics(getProcessedMessageAsync) {
                logTime("getProcessedMessage (id=$id)") {
                    storage.getProcessedMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageAsync) {
                logTime("getMessage (id=$id)") {
                    storage.getMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getEventsSuspend(from: Instant, to: Instant): Iterable<StoredTestEventMetadata> {
        return withContext(coroutineContext) {
            logMetrics(getTestEventsAsync) {
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
            logMetrics(getTestEventsAsync) {
                logTime("Get events parent: $parentId from: $from to: $to") {
                    storage.getTestEventsAsync(parentId, from, to).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
        return withContext(coroutineContext) {
            logMetrics(getTestEventAsync) {
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
            logMetrics(getNearestMessageId) {
                logTime(("getFirstMessageId (timestamp=$timestamp stream=$stream direction=${direction.label} )")) {
                    storage.getNearestMessageId(stream, direction, timestamp, timelineDirection)
                }
            }
        }
    }

    suspend fun getMessageBatchSuspend(id: StoredMessageId): Collection<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageBatchAsync) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    storage.getMessageBatchAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
        return withContext(Dispatchers.IO) {
            logMetrics(getTestEventIdsByMessageIdAsync) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    linker.getTestEventIdsByMessageIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageIdsSuspend(id: StoredTestEventId): Collection<StoredMessageId> {
        return withContext(Dispatchers.IO) {
            logMetrics(getMessageIdsByTestEventIdAsync) {
                logTime("getMessageIdsByTestEventId (id=$id)") {
                    linker.getMessageIdsByTestEventIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageStreams(): Collection<String> {
        return withContext(Dispatchers.IO) {
            logMetrics(getStreams) {
                logTime("getStreams") {
                    storage.streams
                }
            } ?: emptyList()
        }
    }
}
