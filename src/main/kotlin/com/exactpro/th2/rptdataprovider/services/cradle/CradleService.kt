/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
 */


package com.exactpro.th2.rptdataprovider.services.cradle

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.Order
import com.exactpro.cradle.PageInfo
import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
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
import com.exactpro.th2.rptdataprovider.toGroupedMessageFilter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class CradleService(configuration: Configuration, cradleManager: CradleManager) {

    companion object {
        val logger = KotlinLogging.logger {}

        private val getMessagesBatches: Metrics = Metrics("get_messages_batches_async", "getMessagesBatchesAsync")

        private val getMapAliasToGroupAsyncMetric: Metrics =
            Metrics("map_session_aslias_to_group_async", "findPage;getSessionGroups;getGroupedMessageBatches")
        private val getMessageAsyncMetric: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val getTestEventsAsyncMetric: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val getTestEventAsyncMetric: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val getStreamsMetric: Metrics = Metrics("get_streams", "getStreams")

        // source interval changes to exclude intersection to next or previous page
        private fun PageInfo.toInterval(): Interval = Interval(
            started?.plusNanos(1) ?: Instant.MIN,
            ended?.minusNanos(1) ?: Instant.MAX
        )
    }

    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)

    private val searchBySessionGroup = configuration.searchBySessionGroup.value.toBoolean()
    private val aliasToGroupCacheSize = configuration.aliasToGroupCacheSize.value.toLong()

    private val bookToCache = ConcurrentHashMap<String, Cache<String, String>>()

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
                    if (searchBySessionGroup) {
                        val group = getSessionGroupSuspend(filter)
                        val groupedMessageFilter = filter.toGroupedMessageFilter(group).also {
                            logger.debug { "Start searching group batches by $it" }
                        }
                        storage.getGroupedMessageBatchesAsync(groupedMessageFilter).await().asSequence()
                            .map { batch ->
                                batch.messages.asSequence().filter { message ->
                                    filter.sessionAlias == message.sessionAlias
                                            && filter.direction == message.direction
                                            && filter.timestampFrom?.check(message.timestamp) ?: true
                                            && filter.timestampTo?.check(message.timestamp) ?: true
                                }.toList()
                                    .run {
                                        if (isEmpty()) {
                                            StoredMessageBatch()
                                        } else {
                                            StoredMessageBatch(
                                                this,
                                                storage.findPage(batch.bookId, batch.recDate).id,
                                                batch.recDate
                                            )
                                        }
                                    }
                            }.filterNot(StoredMessageBatch::isEmpty)
                            .asIterable()
                    } else {
                        storage.getMessageBatchesAsync(filter).await().asIterable()
                    }
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

    private suspend fun getSessionGroupSuspend(
        bookId: BookId,
        from: Instant?,
        to: Instant?,
        sessionAlias: String
    ): String? {
        val cache = bookToCache.computeIfAbsent(bookId.name) {
            manager.createCache(
                "aliasToGroup(${bookId.name})",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(
                    String::class.java,
                    String::class.java,
                    ResourcePoolsBuilder.heap(aliasToGroupCacheSize)
                ).build()
            )

        }
        return cache.get(sessionAlias)
            ?: run {
                withContext(cradleDispatcher) {
                    logMetrics(getMapAliasToGroupAsyncMetric) {
                        logTime("getSessionGroup (book=${bookId.name}, from=${from}, to=${to}, session alias=${sessionAlias})") {
                            val interval = Interval(from ?: Instant.MIN, to ?: Instant.MAX)
                            logger.debug { "Strat searching '$sessionAlias' session alias in cradle in [${interval.start}, ${interval.end}] interval" }
                            // getPagesAsync method is used instead of getPage because the first one return all pages touched by interval
                            storage.getPagesAsync(bookId, interval).get().asSequence()
                                .map { pageInfo -> pageInfo.toInterval() }
                                .filter { pageInterval ->
                                    storage.getSessionAliases(bookId, pageInterval).asSequence()
                                        .any { alias -> alias == sessionAlias }
                                }.firstOrNull()
                                ?.let searchInGroups@ { pageInterval ->
                                    cache.get(sessionAlias)?.let { group ->
                                        logger.debug { "Another coroutine has dound session '$sessionAlias' alias to '$group' group pair" }
                                        return@searchInGroups group
                                    }
                                    logger.debug { "Strat searching session group by '$sessionAlias' alias in cradle in (${pageInterval.start}, ${pageInterval.end}) interval" }
                                    storage.getSessionGroups(bookId, pageInterval).asSequence()
                                        .flatMap { group ->
                                            storage.getGroupedMessageBatches(
                                                GroupedMessageFilter.builder()
                                                    .bookId(bookId)
                                                    .timestampFrom().isGreaterThan(pageInterval.start)
                                                    .timestampTo().isLessThan(pageInterval.end)
                                                    .groupName(group)
                                                    .order(Order.DIRECT)
                                                    .build()
                                            ).asSequence()
                                        }.forEach searchInBatch@{ batch ->
                                            cache.get(sessionAlias)?.let { group ->
                                                logger.debug { "Another coroutine has dound session '$sessionAlias' alias to '$group' group pair" }
                                                return@searchInGroups group
                                            }
                                            logger.debug { "Search session group by '$sessionAlias' alias in grouped batch" }
                                            batch.messages.forEach { message ->
                                                cache.putIfAbsent(message.sessionAlias, batch.group) ?: run {
                                                    logger.info { "Put session '${message.sessionAlias}' alias to '${batch.group}' group to cache" }
                                                }
                                                if (sessionAlias == message.sessionAlias) {
                                                    logger.debug { "Found session '${message.sessionAlias}' alias to '${batch.group}' group pair" }
                                                    return@searchInGroups batch.group
                                                }
                                            }
                                        }
                                    error("Mapping between a session group and the '${sessionAlias}' session alias isn't found, book: ${bookId.name}, [from: $from, to: $to]")
                                } ?: run {
                                    logger.debug { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval interval" }
                                    null
                                }
                        }
                    }
                }
            }
    }

    private suspend fun getSessionGroupSuspend(
        filter: MessageFilter
    ): String? = getSessionGroupSuspend(
        filter.bookId,
        filter.timestampFrom?.value,
        filter.timestampTo?.value,
        filter.sessionAlias
    )

    private suspend fun getSessionGroupSuspend(id: StoredMessageId): String? =
        getSessionGroupSuspend(id.bookId, id.timestamp, id.timestamp, id.sessionAlias)

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(getMessageAsyncMetric) {
                logTime("getMessage (id=$id)") {
                    if (searchBySessionGroup) {
                        getSessionGroupSuspend(id)?.run {
                            storage.getGroupedMessageBatches(GroupedMessageFilter.builder().apply {
                                bookId(id.bookId)
                                timestampFrom().isGreaterThanOrEqualTo(id.timestamp)
                                timestampTo().isLessThanOrEqualTo(id.timestamp)
                                groupName(this@run)
                            }.build()).asSequence()
                                .flatMap(StoredGroupedMessageBatch::getMessages)
                                .filter { message -> id == message.id }
                                .firstOrNull()
                        }
                    } else {
                        storage.getMessageAsync(id).await()
                    }
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
                    eventFilter.parentId = parentId
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
            } ?: emptyList()
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
