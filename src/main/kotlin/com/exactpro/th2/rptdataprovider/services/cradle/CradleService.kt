/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.convertToString
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executors

class CradleService(configuration: Configuration, private val cradleManager: CradleManager) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesAsyncMetric: Metrics = Metrics("get_messages_async", "getMessagesAsync")

        private val getMessagesBatches: Metrics = Metrics("get_messages_batches_async", "getMessagesBatchesAsync")

        private val getProcessedMessageAsyncMetric: Metrics =
            Metrics("get_processed_message_async", "getProcessedMessageAsync")
        private val getMessageAsyncMetric: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncMetric: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncMetric: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getTestCompletedEventAsyncMetric: Metrics =
            Metrics("get_completed_test_event_async", "getCompleteTestEventsAsync")
        private val getNearestMessageIdMetric: Metrics = Metrics("get_nearest_message_id", "getNearestMessageId")
        private val getMessageBatchAsyncMetric: Metrics = Metrics("get_message_batch_async", "getMessageBatchAsync")
        private val getTestEventIdsByMessageIdAsyncMetric: Metrics =
            Metrics("get_test_event_ids_by_message_id_async", "getTestEventIdsByMessageIdAsync")
        private val getMessageIdsByTestEventIdAsyncMetric: Metrics =
            Metrics("get_message_ids_by_test_event_id_async", "getMessageIdsByTestEventIdAsync")
        private val getStreamsMetric: Metrics =
            Metrics("get_streams", "getStreams")
    }


    private val cradleDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()

    private val storage = cradleManager.storage
    private val linker = cradleManager.storage.testEventsMessagesLinker

    // FIXME: thread name
    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()

    suspend fun getMessagesSuspend(filter: StoredMessageFilter): Iterable<StoredMessage> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessagesAsyncMetric) {
                logTime("getMessages (filter=${filter.convertToString()})") {
                    storage.getMessagesAsync(filter).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getMessagesBatchesSuspend(filter: StoredMessageFilter): Iterable<StoredMessageBatch> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessagesBatches) {
                logTime("getMessagesBatches (filter=${filter.convertToString()})") {
                    storage.getMessagesBatchesAsync(filter).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getProcessedMessageAsyncMetric) {
                logTime("getProcessedMessage (id=$id)") {
                    storage.getProcessedMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageAsyncMetric) {
                logTime("getMessage (id=$id)") {
                    storage.getMessageAsync(id).await()
                }
            }
        }
    }

    suspend fun getEventsSuspend(
        from: Instant,
        to: Instant,
        order: Order = Order.DIRECT
    ): Iterable<StoredTestEventMetadata> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events from: $from to: $to") {
                    storage.getTestEventsAsync(from, to, order).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventsSuspend(
        parentId: StoredTestEventId,
        from: Instant,
        to: Instant,
        order: Order = Order.DIRECT
    ): Iterable<StoredTestEventMetadata> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events parent: $parentId from: $from to: $to") {
                    storage.getTestEventsAsync(parentId, from, to, order).await()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventAsyncMetric) {
                logTime("getTestEvent (id=$id)") {
                    storage.getTestEventAsync(id).await()
                }
            }
        }
    }

    suspend fun getCompletedEventSuspend(ids: Set<StoredTestEventId>): Iterable<StoredTestEventWrapper> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestCompletedEventAsyncMetric) {
                logTime("getCompleteTestEvents (id=$ids)") {
                    storage.getCompleteTestEventsAsync(ids).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getFirstMessageIdSuspend(
        timestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation
    ): StoredMessageId? {
        return withContext(cradleDispatcher) {
            logMetrics(getNearestMessageIdMetric) {
                logTime(("getFirstMessageId (timestamp=$timestamp stream=$stream direction=${direction.label} )")) {
                    storage.getNearestMessageId(stream, direction, timestamp, timelineDirection).also {
                        logger.debug { "The $it message id for stream = $stream timestamp = $timestamp (from storage)" }
                    }
                }
            }
        }.also {
            logger.debug { "The $it message id for stream = $stream timestamp = $timestamp (in IO thread)" }
        }
    }

    suspend fun getFirstMessageIdSuspend(
        timestamp: Instant,
        stream: StreamName,
        timelineDirection: TimeRelation
    ): StoredMessageId? {
        return getFirstMessageIdSuspend(timestamp, stream.name, stream.direction, timelineDirection)
    }

    suspend fun getMessageBatchSuspend(id: StoredMessageId): Collection<StoredMessage> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageBatchAsyncMetric) {
                logTime("getMessageBatchByMessageId (id=$id)") {
                    storage.getMessageBatchAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventIdsByMessageIdAsyncMetric) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    linker.getTestEventIdsByMessageIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageIdsSuspend(id: StoredTestEventId): Collection<StoredMessageId> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageIdsByTestEventIdAsyncMetric) {
                logTime("getMessageIdsByTestEventId (id=$id)") {
                    linker.getMessageIdsByTestEventIdAsync(id).await()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageStreams(): Collection<String> {
        return withContext(cradleDispatcher) {
            logMetrics(getStreamsMetric) {
                logTime("getStreams") {
                    storage.streams
                }
            } ?: emptyList()
        }
    }

    suspend fun getFirstMessageIndex(stream: String, direction: Direction): Long {
        return withContext(cradleDispatcher) {
            logTime("getFirstIdInStream") {
                storage.getFirstMessageIndex(stream, direction)
            }
        } ?: -1
    }
}
