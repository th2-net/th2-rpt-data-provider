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
import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.*
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
import java.util.concurrent.Executors

class CradleService(configuration: Configuration, private val cradleManager: CradleManager) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesAsyncMetric: Metrics = Metrics("get_messages_async", "getMessagesAsync")

        private val getMessagesBatches: Metrics = Metrics("get_messages_batches_async", "getMessagesBatchesAsync")

        private val getMessageAsyncMetric: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncMetric: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncMetric: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getStreamsMetric: Metrics =
            Metrics("get_streams", "getStreams")
    }


    private val cradleDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()

    private val storage = cradleManager.storage


    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()

    // FIXME:
    // It is not correct to create scope manually inside the suspend function
    // If the function is going to launch extra coroutines it should accept a coroutine scope.
    // Otherwise, the top-level scope will never know about exceptions inside that inner scope.
    // How it should look:
    //
    // fun getMessagesBatchesSuspend(filter: MessageFilter, scope: CoroutineScope): Channel<StoredMessageBatch> {
    //   val channel = Channel<StoredMessageBatch>(1)
    //   scope.launch {
    //      withContext(cradleDispatcher) {
    //        // do all work here
    //      }
    //   }
    //   return channel
    // }
    //
    //FIXME: change cradle api or wrap every blocking iterator the same way
    suspend fun getMessagesBatchesSuspend(filter: MessageFilter): Channel<StoredMessageBatch> {
        val iteratorScope = CoroutineScope(cradleDispatcher)

        return withContext(cradleDispatcher) {
            (logMetrics(getMessagesBatches) {
                logTime("getMessagesBatches (filter=${filter.convertToString()})") {
                    storage.getMessageBatchesAsync(filter).await().asIterable()
                }
            } ?: listOf())
                .let { iterable ->
                    Channel<StoredMessageBatch>(1)
                        .also { channel ->
                            iteratorScope.launch {
                                var error: Throwable? = null
                                try {
                                    iterable.forEach {
                                        logger.trace { "message batch ${it.id} has been received from the iterator" }
                                        channel.send(it)
                                        logger.trace { "message batch ${it.id} has been sent to the channel" }
                                    }
                                } catch (ex: Exception) {
                                    logger.error(ex) { "cannot sent next batch to the channel" }
                                    error = ex
                                } finally {
                                    channel.close(error)
                                    logger.debug(error) { "message batch channel for stream ${filter.sessionAlias}:${filter.direction} has been closed" }
                                }
                            }
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
        eventFilter: TestEventFilter
    ): Iterable<StoredTestEvent> {
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
                storage.listBooks()
                    .filter { it.schemaVersion == CassandraStorageSettings.SCHEMA_VERSION }
                    .map { BookId(it.name) }
            }
        } ?: emptyList()
    }

    suspend fun getEventScopes(bookId: BookId): List<String> {
        return logTime("getEventScopes") {
            storage.getScopes(bookId).filterNotNull().toList()
        } ?: emptyList()
    }
}
