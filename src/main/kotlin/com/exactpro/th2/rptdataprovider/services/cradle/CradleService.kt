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
import com.exactpro.cradle.Order
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
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executors

class CradleService(configuration: Configuration, cradleManager: CradleManager) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesBatches: Metrics = Metrics("get_messages_batches_async", "getMessagesBatchesAsync")

        private val getMessageAsyncMetric: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncMetric: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncMetric: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getTestCompletedEventAsyncMetric: Metrics =
            Metrics("get_completed_test_event_async", "getCompleteTestEventsAsync")
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

    // FIXME: Change thread name patter to something easily identifiable in the logs
    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()

    //FIXME: change cradle api or wrap every blocking iterator the same way
    suspend fun getMessagesBatchesSuspend(filter: StoredMessageFilter): Channel<StoredMessageBatch> {
        val iteratorScope = CoroutineScope(cradleDispatcher)

        return withContext(cradleDispatcher) {
            val result = (logMetrics(getMessagesBatches) {
                logTime("getMessagesBatches (filter=${filter.convertToString()})") {
                    storage.getMessagesBatchesAsync(filter).await()
                }
            } ?: listOf())
                .let { iterable ->
                    val channel = Channel<StoredMessageBatch>(1)
                        .also { channel ->
                            iteratorScope.launch {
                                iterable.forEach {
                                    logger.trace { "message batch ${it.id} has been received from the iterator" }
                                    channel.send(it)
                                    logger.trace { "message batch ${it.id} has been sent to the channel" }
                                }
                                channel.close()
                                logger.debug { "message batch channel for stream ${filter.streamName}:${filter.direction} has been closed" }
                            }
                            logger.trace { "also block ${filter.streamName}:${filter.direction}" }
                        }
                    logger.trace { "let block ${filter.streamName}:${filter.direction}" }
                    channel
                }
            logger.trace { "withContext block ${filter.streamName}:${filter.direction}" }
            result
        }.also {
            logger.trace { "fun block ${filter.streamName}:${filter.direction}" }
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
}
