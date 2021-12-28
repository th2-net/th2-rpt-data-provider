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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.convertToString
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
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
        private val getSoloMessageBatchMetric:Metrics =
            Metrics("get_solo_message_batch_metric","getSoloMessageBatchMetric")
    }


    private val cradleDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()

    private val storage = cradleManager.storage


    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()

    suspend fun getMessagesSuspend(filter: MessageFilter): Iterable<StoredMessage> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessagesAsyncMetric) {
                logTime("getMessages (filter=${filter.convertToString()})") {
                    storage.getMessagesAsync(filter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getMessagesBatchesSuspend(filter: MessageFilter): Iterable<StoredMessageBatch> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessagesBatches) {
                logTime("getMessagesBatches (filter=${filter.convertToString()})") {
                    storage.getMessageBatchesAsync(filter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getProcessedMessageAsyncMetric) {
                logTime("getProcessedMessage (id=$id)") {
                    storage.getMessageAsync(id).await()
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

    suspend fun getEventsSuspend(eventFilter: TestEventFilter): Iterable<StoredTestEvent> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events from: ${eventFilter.startTimestampFrom} to: ${eventFilter.startTimestampTo}") {
                    storage.getTestEventsAsync(eventFilter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventsSuspend(parentId: StoredTestEventId, eventFilter: TestEventFilter): Iterable<StoredTestEvent> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventsAsyncMetric) {
                logTime("Get events parent: $parentId from: ${eventFilter.startTimestampFrom} to: ${eventFilter.startTimestampTo}") {
                    eventFilter.setParentId(parentId)
                    storage.getTestEventsAsync(eventFilter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEvent? {
        return withContext(cradleDispatcher) {
            logMetrics(getTestEventAsyncMetric) {
                logTime("getTestEvent (id=$id)") {
                    storage.getTestEventAsync(id).await()
                }
            }
        }
    }

    suspend fun getCompletedEventSuspend(filter: TestEventFilter): Iterable<StoredTestEvent> {
        return withContext(cradleDispatcher) {
            logMetrics(getTestCompletedEventAsyncMetric) {
                logTime("getCompleteTestEvents (id=$filter)") {
                    storage.getTestEventsAsync(filter).await().asIterable()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageBatchSuspend(filter: MessageFilter): Iterable<StoredMessageBatch> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageBatchAsyncMetric) {
                logTime("getMessageBatchByMessageId (id=$filter)") {
                    storage.getMessageBatchesAsync(filter).await().asIterable()
                }
            } ?: emptyList()
        }
    }

    suspend fun getMessageIdsSuspend(id: StoredTestEventId): Collection<StoredMessageId> {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageIdsByTestEventIdAsyncMetric) {
                logTime("getMessageIdsByTestEventId (id=$id)") {
                    val storageTestEvent = storage.getTestEventAsync(id).await()
                    storageTestEvent.messages
                }
            } ?: emptyList<StoredMessageId>()
        }
    }

    suspend fun getSessionAliases(bookId: BookId): Collection<String> {
        return withContext(cradleDispatcher) {
            logMetrics(getStreamsMetric) {
                logTime("getStreams") {
                    storage.getSessionAliases(bookId)
                }
            } ?: emptyList<String>()
        }
    }

    suspend fun getBookIds(): List<BookId> {
        return logMetrics(getStreamsMetric) {
            logTime("getBookIds") {
                storage.books.map { it.id }
            }
        } ?: emptyList()
    }

    suspend fun getEventScopes(bookId: BookId): List<String> {
        return logTime("getEventScopes") {
            storage.getScopes(bookId).filterNotNull().toList()
        } ?: emptyList()
    }

    suspend fun getSingleMessageBatch(storedMessageId: StoredMessageId):StoredMessageBatch? {
        return logMetrics(getSoloMessageBatchMetric){
            logTime("getSoloMessageBatch") {
                storage.getMessageBatch(storedMessageId)
            }
        }
    }
}
