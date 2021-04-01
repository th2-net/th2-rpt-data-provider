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

import com.datastax.oss.driver.api.core.DriverTimeoutException
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
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.coroutines.coroutineContext

class CradleService(configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesAsyncMetric: Metrics = Metrics("get_messages_async", "getMessagesAsync")
        private val getProcessedMessageAsyncMetric: Metrics =
            Metrics("get_processed_message_async", "getProcessedMessageAsync")
        private val getMessageAsyncMetric: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncMetric: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncMetric: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getNearestMessageIdMetric: Metrics = Metrics("get_nearest_message_id", "getNearestMessageId")
        private val getMessageBatchAsyncMetric: Metrics = Metrics("get_message_batch_async", "getMessageBatchAsync")
        private val getTestEventIdsByMessageIdAsyncMetric: Metrics =
            Metrics("get_test_event_ids_by_message_id_async", "getTestEventIdsByMessageIdAsync")
        private val getMessageIdsByTestEventIdAsyncMetric: Metrics =
            Metrics("get_message_ids_by_test_event_id_async", "getMessageIdsByTestEventIdAsync")
        private val getStreamsMetric: Metrics =
            Metrics("get_streams", "getStreams")
    }

    private val cradleManager: CradleManager = configuration.cradleManager

    private val cradleDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()

    private val storage = cradleManager.storage
    private val linker = cradleManager.storage.testEventsMessagesLinker

    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()
    private val dbRetryDelay = configuration.dbRetryDelay.value.toLong()


    private suspend fun <T> simpleRetry(useRetry: Boolean, request: suspend () -> T?): T? {
        return withContext(coroutineContext) {
            var result: T? = null
            var needRetry = useRetry
            do {
                try {
                    result = request.invoke()
                    needRetry = false
                } catch (e: Exception) {
                    logger.error(e) { }
                    when (ExceptionUtils.getRootCause(e)) {
                        is DriverTimeoutException -> {
                            logger.debug { "try to reconnect" }
                            delay(dbRetryDelay)
                        }
                        else -> throw e
                    }
                }
            } while (needRetry)

            result
        }
    }

    suspend fun getMessagesSuspend(filter: StoredMessageFilter, needRetry: Boolean = false): Iterable<StoredMessage> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessagesAsyncMetric) {
                logTime("getMessages (filter=${filter.convertToString()})") {
                    simpleRetry(needRetry) {
                        storage.getMessagesAsync(filter).await()
                    }
                }
            } ?: listOf()
        }
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId, needRetry: Boolean = false): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getProcessedMessageAsyncMetric) {
                logTime("getProcessedMessage (id=$id)") {
                    simpleRetry(needRetry) {
                        storage.getProcessedMessageAsync(id).await()
                    }
                }
            }
        }
    }

    suspend fun getMessageSuspend(id: StoredMessageId, needRetry: Boolean = false): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageAsyncMetric) {
                logTime("getMessage (id=$id)") {
                    simpleRetry(needRetry) {
                        storage.getMessageAsync(id).await()
                    }
                }
            }
        }
    }

    suspend fun getEventsSuspend(
        from: Instant,
        to: Instant,
        needRetry: Boolean = false
    ): Iterable<StoredTestEventMetadata> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events from: $from to: $to") {
                    simpleRetry(needRetry) {
                        storage.getTestEventsAsync(from, to).await()
                    }
                }
            } ?: listOf()
        }
    }

    suspend fun getEventsSuspend(
        parentId: StoredTestEventId,
        from: Instant,
        to: Instant,
        needRetry: Boolean = false
    ): Iterable<StoredTestEventMetadata> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events parent: $parentId from: $from to: $to") {
                    simpleRetry(needRetry) {
                        storage.getTestEventsAsync(parentId, from, to).await()
                    }
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(
        id: StoredTestEventId,
        needRetry: Boolean = false
    ): StoredTestEventWrapper? {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventAsyncMetric) {
                logTime("getTestEvent (id=$id)") {
                    simpleRetry(needRetry) {
                        storage.getTestEventAsync(id).await()
                    }
                }
            }
        }
    }

    suspend fun getFirstMessageIdSuspend(
        timestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation,
        needRetry: Boolean = false
    ): StoredMessageId? {
        return withContext(cradleDispatcher) {
            logMetrics(getNearestMessageIdMetric) {
                logTime(("getFirstMessageId (timestamp=$timestamp stream=$stream direction=${direction.label} )")) {
                    simpleRetry(needRetry) {
                        storage.getNearestMessageId(stream, direction, timestamp, timelineDirection)
                    }
                }
            }
        }
    }

    suspend fun getMessageBatchSuspend(
        id: StoredMessageId,
        needRetry: Boolean = false
    ): Collection<StoredMessage> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageBatchAsyncMetric) {
                logTime("getMessageBatchByMessageId (id=$id)") {
                    simpleRetry(needRetry) {
                        storage.getMessageBatchAsync(id).await()
                    }
                }
            } ?: emptyList()
        }
    }

    suspend fun getEventIdsSuspend(
        id: StoredMessageId,
        needRetry: Boolean = false
    ): Collection<StoredTestEventId> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventIdsByMessageIdAsyncMetric) {
                logTime("getTestEventIdsByMessageId (id=$id)") {
                    simpleRetry(needRetry) {
                        linker.getTestEventIdsByMessageIdAsync(id).await()
                    }
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageIdsSuspend(
        id: StoredTestEventId,
        needRetry: Boolean = false
    ): Collection<StoredMessageId> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageIdsByTestEventIdAsyncMetric) {
                logTime("getMessageIdsByTestEventId (id=$id)") {
                    simpleRetry(needRetry) {
                        linker.getMessageIdsByTestEventIdAsync(id).await()
                    }
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageStreams(needRetry: Boolean = false): Collection<String> {
        return withContext(cradleDispatcher) {
            logMetrics(getStreamsMetric) {
                logTime("getStreams") {
                    simpleRetry(needRetry) {
                        storage.streams
                    }
                }
            } ?: emptyList()
        }
    }
}
