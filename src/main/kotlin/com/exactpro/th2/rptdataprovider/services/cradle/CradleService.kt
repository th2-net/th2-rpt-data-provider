/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.Direction.SECOND
import com.exactpro.cradle.FrameType
import com.exactpro.cradle.PageInfo
import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.channels.ReceiveChannel
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class CradleService(configuration: Configuration, cradleManager: CradleManager) {

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private val CACHE_MANAGER = CacheManagerBuilder.newCacheManagerBuilder().build(true)
        private val GET_MAP_ALIAS_TO_GROUP_ASYNC_METRIC: Metrics =
            Metrics("map_session_alias_to_group_async", "findPage;getSessionGroups;getGroupedMessageBatches")
        private val CRADLE_MIN_TIMESTAMP: Instant = Instant.ofEpochMilli(0L)
        // FIXME: minusMillis(FrameType.values().asSequence().map(FrameType::getMillisInFrame).maxOf(Long::toLong)) is used as workaround for `long overflow` problem in CradleStorage.getMessageCountersAsync method.
        // CradleStorage.getMessageCountersAsync add FrameType.getMillisInFrame to get the last next lest interval
        // Cradle should support null value for from / to Interval properties
        private val CRADLE_MAX_TIMESTAMP: Instant = Instant.ofEpochMilli(Long.MAX_VALUE).minusMillis(
            FrameType.values().asSequence().map(FrameType::getMillisInFrame).maxOf(Long::toLong)
        )

        private val GET_MESSAGES_BATCHES: Metrics = Metrics("get_messages_batches_async", "getMessagesBatchesAsync")
        private val GET_MESSAGE_ASYNC_METRIC: Metrics = Metrics("get_message_async", "getMessageAsync")
        private val GET_TEST_EVENTS_ASYNC_METRIC: Metrics = Metrics("get_test_events_async", "getTestEventsAsync")
        private val GET_TEST_EVENT_ASYNC_METRIC: Metrics = Metrics("get_test_event_async", "getTestEventAsync")
        private val GET_STREAMS_METRIC: Metrics = Metrics("get_streams", "getStreams")

        private fun createInterval(from: Instant?, to: Instant?): Interval {
            return Interval(
                if (from == null || from.isBefore(CRADLE_MIN_TIMESTAMP)) CRADLE_MIN_TIMESTAMP else from,
                if (to == null || to.isAfter(CRADLE_MAX_TIMESTAMP)) CRADLE_MAX_TIMESTAMP else to,
            )
        }

        // source interval changes to exclude intersection to next or previous page
        private fun PageInfo.toInterval(): Interval = Interval(
            started?.plusNanos(1) ?: CRADLE_MIN_TIMESTAMP,
            ended?.minusNanos(1) ?: CRADLE_MAX_TIMESTAMP
        )

        private fun Interval.print(): String = "[$start, $end]"
        private fun max(instantA: Instant, instantB: Instant):Instant = if (instantA.isAfter(instantB)) {
            instantA
        } else {
            instantB
        }

        private fun min(instantA: Instant, instantB: Instant):Instant = if (instantA.isBefore(instantB)) {
            instantA
        } else {
            instantB
        }

        private fun intersection(intervalA: Interval, intervalB: Interval): Interval {
            require(!intervalA.start.isAfter(intervalB.end)
                    && !intervalB.start.isAfter(intervalA.end)) {
                "${intervalA.print()}, ${intervalB.print()} intervals aren't intersection"
            }
            return Interval(
                max(intervalA.start, intervalB.start),
                min(intervalA.end, intervalB.end)
            )
        }
    }

    private val bookToCache = ConcurrentHashMap<String, Cache<String, String>>()

    private val aliasToGroupCacheSize = configuration.aliasToGroupCacheSize.value.toLong()

    private val cradleDispatcherPoolSize = configuration.cradleDispatcherPoolSize.value.toInt()

    private val storage: CradleStorage = cradleManager.storage

    private val cradleDispatcher = Executors.newFixedThreadPool(cradleDispatcherPoolSize).asCoroutineDispatcher()

    suspend fun getGroupedMessages(
        scope: CoroutineScope,
        filter: GroupedMessageFilter,
    ): ReceiveChannel<StoredGroupedMessageBatch> {
        val channel = Channel<StoredGroupedMessageBatch>(1)
        scope.launch {
            withContext(cradleDispatcher) {
                try {
                    storage.getGroupedMessageBatches(filter).forEach { batch ->
                        K_LOGGER.trace {
                            "message batch has been received from the iterator, " +
                                    "group: ${batch.group}, start: ${batch.firstTimestamp}, " +
                                    "end: ${batch.lastTimestamp}, request order: ${filter.order}"
                        }
                        channel.send(batch)
                        K_LOGGER.trace {
                            "message batch has been sent to the channel, " +
                                    "group: ${batch.group}, start: ${batch.firstTimestamp}, " +
                                    "end: ${batch.lastTimestamp}, request order: ${filter.order}"
                        }
                    }
                } finally {
                    channel.close()
                }
            }
        }
        return channel
    }

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(cradleDispatcher) {
            logMetrics(GET_MESSAGE_ASYNC_METRIC) {
                logTime("getMessage (id=$id)") {
                    getMessageBatches(id)
                }
            }
        }
    }

    suspend fun getEventsSuspend(
        eventFilter: TestEventFilter
    ): Iterable<StoredTestEvent> {
        return withContext(cradleDispatcher) {
            logMetrics(GET_TEST_EVENTS_ASYNC_METRIC) {
                logTime("Get events from: ${eventFilter.startTimestampFrom} to: ${eventFilter.startTimestampTo}") {
                    storage.getTestEventsAsync(eventFilter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventsSuspend(parentId: StoredTestEventId, eventFilter: TestEventFilter): Iterable<StoredTestEvent> {
        return withContext(cradleDispatcher) {
            logMetrics(GET_TEST_EVENTS_ASYNC_METRIC) {
                logTime("Get events parent: $parentId from: ${eventFilter.startTimestampFrom} to: ${eventFilter.startTimestampTo}") {
                    eventFilter.parentId = parentId
                    storage.getTestEventsAsync(eventFilter).await().asIterable()
                }
            } ?: listOf()
        }
    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEvent? {
        return withContext(cradleDispatcher) {
            logMetrics(GET_TEST_EVENT_ASYNC_METRIC) {
                logTime("getTestEvent (id=$id)") {
                    storage.getTestEventAsync(id).await()
                }
            }
        }
    }

    suspend fun getSessionAliases(bookId: BookId): Collection<String> {
        return withContext(cradleDispatcher) {
            logMetrics(GET_STREAMS_METRIC) {
                logTime("getStreams") {
                    storage.getSessionAliases(bookId)
                }
            } ?: emptyList()
        }
    }

    suspend fun getBookIds(): List<BookId> {
        return logMetrics(GET_STREAMS_METRIC) {
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

    private suspend fun getMessageBatches(
        id: StoredMessageId
    ): StoredMessage? = getSessionGroup(id)?.run {
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

    suspend fun getSessionGroup(
        bookId: BookId,
        sessionAlias: String,
        from: Instant?,
        to: Instant?
    ): String? = withContext(cradleDispatcher) {
            val cache = bookToCache.computeIfAbsent(bookId.name) {
                CACHE_MANAGER.createCache(
                    "aliasToGroup(${bookId.name})",
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        String::class.java,
                        String::class.java,
                        ResourcePoolsBuilder.heap(aliasToGroupCacheSize)
                    ).build()
                )
            }
            cache.get(sessionAlias) ?: searchSessionGroup(bookId, from, to, sessionAlias, cache)
        }

    private suspend fun searchSessionGroup(
        bookId: BookId,
        from: Instant?,
        to: Instant?,
        sessionAlias: String,
        cache: Cache<String, String>
    ): String? = logMetrics(GET_MAP_ALIAS_TO_GROUP_ASYNC_METRIC) {
        logTime("getSessionGroup (book=${bookId.name}, from=${from}, to=${to}, session alias=${sessionAlias})") {
            val interval = createInterval(from, to)

            val sessionGroup: String? = eachSessionGroupByStatistics(sessionAlias, interval, bookId, cache)
                ?: eachSessionGroupByPage(sessionAlias, interval, bookId, cache)

            if (sessionGroup == null) {
                cache.get(sessionAlias)?.let { group ->
                    K_LOGGER.debug { "Another coroutine has found '$sessionAlias' session alias to '$group' group pair" }
                    return@logTime group
                } ?: error("Mapping between a session group and the '${sessionAlias}' alias isn't found, book: ${bookId.name}, [from: $from, to: $to]")
            } else {
                return@logTime sessionGroup
            }
        }
    }

    private suspend fun eachSessionGroupByPage(
        sessionAlias: String,
        interval: Interval,
        bookId: BookId,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Start searching '$sessionAlias' session alias in cradle in [${interval.start}, ${interval.end}] interval by page" }
        val pageInterval = storage.getPagesAsync(bookId, interval).await().asSequence()
            .map { pageInfo -> pageInfo.toInterval() }
            .filter { pageInterval ->
                storage.getSessionAliases(bookId, pageInterval).asSequence()
                    .any { alias -> alias == sessionAlias }
            }.firstOrNull()

        cache.get(sessionAlias)?.let { group ->
            K_LOGGER.debug { "Another coroutine has found '$sessionAlias' session alias to '$group' group pair" }
            return group
        }

        if (pageInterval == null) {
            K_LOGGER.info { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval" }
            return null
        }

        val targetInterval = intersection(interval, pageInterval)

        return searchSessionGroupByGroupedMessage(sessionAlias, targetInterval, bookId, cache).also {
            if (it == null) {
                K_LOGGER.warn { "Mapping between a session group and the '${sessionAlias}' alias isn't found by page, book: ${bookId.name}, interval: [${interval.start}, ${interval.end}] " }
            }
        }
    }

    private suspend fun eachSessionGroupByStatistics(
        sessionAlias: String,
        interval: Interval,
        bookId: BookId,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Start searching '$sessionAlias' session alias counters in cradle in [${interval.start}, ${interval.end}] interval, ${FrameType.TYPE_100MS} frame type" }
        val counter = getCounterSample(bookId, sessionAlias, FIRST, interval)
            ?: getCounterSample(bookId, sessionAlias, SECOND, interval)

        cache.get(sessionAlias)?.let { group ->
            K_LOGGER.debug { "Another coroutine has found '$sessionAlias' session alias to '$group' group pair" }
            return group
        }

        if (counter == null) {
            K_LOGGER.info { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval" }
            return null
        }

        val shortInterval = Interval(counter.frameStart, counter.frameStart.plusMillis(FrameType.TYPE_100MS.millisInFrame))
        return searchSessionGroupByGroupedMessage(sessionAlias, shortInterval, bookId, cache).also {
            if (it == null) {
                K_LOGGER.warn { "Mapping between a session group and the '${sessionAlias}' alias isn't found by statistics, book: ${bookId.name}, interval: [${interval.start}, ${interval.end}] " }
            }
        }
    }

    private suspend fun getCounterSample(
        bookId: BookId,
        sessionAlias: String,
        direction: Direction,
        interval: Interval,
    ) = storage.getMessageCountersAsync(bookId, sessionAlias, direction, FrameType.TYPE_100MS, interval)
        .await().asSequence().firstOrNull()

    private suspend fun searchSessionGroupByGroupedMessage(
        sessionAlias: String,
        shortInterval: Interval,
        bookId: BookId,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Start searching session group by '$sessionAlias' alias in cradle in (${shortInterval.start}, ${shortInterval.end}) interval" }
        storage.getSessionGroupsAsync(bookId, shortInterval).await().asSequence()
            .flatMap { group ->
                storage.getGroupedMessageBatches(
                    GroupedMessageFilter.builder()
                        .bookId(bookId)
                        .timestampFrom().isGreaterThanOrEqualTo(shortInterval.start)
                        .timestampTo().isLessThanOrEqualTo(shortInterval.end)
                        .groupName(group)
                        .build()
                ).asSequence()
            }.forEach searchInBatch@{ batch ->
                cache.get(sessionAlias)?.let { group ->
                    K_LOGGER.debug { "Another coroutine has found '$sessionAlias' session alias to '$group' group pair" }
                    return group
                }
                K_LOGGER.debug { "Search session group by '$sessionAlias' alias in grouped batch" }
                batch.messages.forEach { message ->
                    cache.putIfAbsent(message.sessionAlias, batch.group) ?: run {
                        K_LOGGER.info { "Put '${message.sessionAlias}' session alias to '${batch.group}' group to cache" }
                        if (sessionAlias == message.sessionAlias) {
                            K_LOGGER.debug { "Found '${message.sessionAlias}' session alias to '${batch.group}' group pair" }
                        }
                    }
                }
            }
        return null
    }

    private suspend fun getSessionGroup(
        id: StoredMessageId
    ): String? = getSessionGroup(id.bookId, id.sessionAlias, id.timestamp, id.timestamp)
}
