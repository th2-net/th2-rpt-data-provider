/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.Direction
import com.exactpro.cradle.FrameType
import com.exactpro.cradle.PageInfo
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.logTime
import com.exactpro.th2.rptdataprovider.toGroupedMessageFilter
import kotlinx.coroutines.future.await
import io.github.oshai.kotlinlogging.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class MessageGroupCradleService(
    configuration: Configuration,
    cradleManager: CradleManager
): CradleService(
    configuration,
    cradleManager,
) {

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private val CACHE_MANAGER = CacheManagerBuilder.newCacheManagerBuilder().build(true)

        private val GET_MAP_ALIAS_TO_GROUP_ASYNC_METRIC: Metrics =
            Metrics("map_session_aslias_to_group_async", "findPage;getSessionGroups;getGroupedMessageBatches")

        private fun createInterval(from: Instant?, to: Instant?): Interval {
            return Interval(
                if (from == null || from.isBefore(CRADLE_MIN_TIMESTAMP)) CRADLE_MIN_TIMESTAMP else from,
                if (to == null || to.isAfter(CRADLE_MAX_TIMESTAMP)) CRADLE_MAX_TIMESTAMP else to,
            )
        }

        private val CRADLE_MIN_TIMESTAMP: Instant = Instant.ofEpochMilli(0L)
        // FIXME: minusMillis(FrameType.values().asSequence().map(FrameType::getMillisInFrame).maxOf(Long::toLong)) is used as workaround for `long overflow` problem in CradleStorage.getMessageCountersAsync method.
        // CradleStorage.getMessageCountersAsync add FrameType.getMillisInFrame to get the last next lest interval
        // Cradle should support null value for from / to Interval properties
        private val CRADLE_MAX_TIMESTAMP: Instant = Instant.ofEpochMilli(Long.MAX_VALUE).minusMillis(FrameType.values().asSequence().map(FrameType::getMillisInFrame).maxOf(Long::toLong))

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
            return Interval(max(intervalA.start, intervalB.start), min(intervalA.end, intervalB.end))
        }
    }

    private val aliasToGroupCacheSize = configuration.aliasToGroupCacheSize.value.toLong()

    private val bookToCache = ConcurrentHashMap<String, Cache<String, String>>()

    override suspend fun getMessageBatches(
        filter: MessageFilter
    ): Sequence<StoredMessageBatch> =
        getSessionGroupSuspend(filter)?.let { group ->
            val groupedMessageFilter = filter.toGroupedMessageFilter(group).also {
                K_LOGGER.debug { "Start searching group batches by $it" }
            }
            storage.getGroupedMessageBatchesAsync(groupedMessageFilter).await().asSequence()
                .mapNotNull { batch ->
                    val messages = batch.messages.filter { message ->
                        filter.sessionAlias == message.sessionAlias
                                && filter.direction == message.direction
                                && filter.timestampFrom?.check(message.timestamp) ?: true
                                && filter.timestampTo?.check(message.timestamp) ?: true
                    }
                    if (messages.isEmpty()) {
                        null
                    } else {
                        StoredMessageBatch(
                            messages,
                            storage.findPage(batch.bookId, batch.recDate).id,
                            batch.recDate
                        )
                    }
                }

        } ?: emptySequence()

    override suspend fun getMessageBatches(
        id: StoredMessageId
    ): StoredMessage? = getSessionGroupSuspend(id)?.run {
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

    private suspend fun getSessionGroupSuspend(
        bookId: BookId,
        from: Instant?,
        to: Instant?,
        sessionAlias: String,
        direction: Direction
    ): String? {
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
        return cache.get(sessionAlias) ?: searchSessionGroup(bookId, from, to, sessionAlias, direction, cache)
    }

    private suspend fun searchSessionGroup(
        bookId: BookId,
        from: Instant?,
        to: Instant?,
        sessionAlias: String,
        direction: Direction,
        cache: Cache<String, String>
    ): String? = logMetrics(GET_MAP_ALIAS_TO_GROUP_ASYNC_METRIC) {
        logTime("getSessionGroup (book=${bookId.name}, from=${from}, to=${to}, session alias=${sessionAlias})") {
            val interval = createInterval(from, to)

            val sessionGroup: String? = seachSessionGorupByStatistics(sessionAlias, interval, bookId, direction, cache)
                ?: seachSessionGorupByPage(sessionAlias, interval, bookId, cache)

            if (sessionGroup == null) {
                cache.get(sessionAlias)?.let { group ->
                    K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
                    return@logTime group
                } ?: error("Mapping between a session group and the '${sessionAlias}' alias isn't found, book: ${bookId.name}, [from: $from, to: $to]")
            } else {
                return@logTime sessionGroup
            }
        }
    }

    private suspend fun seachSessionGorupByStatistics(
        sessionAlias: String,
        interval: Interval,
        bookId: BookId,
        direction: Direction,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Strat searching '$sessionAlias' session alias counters in cradle in [${interval.start}, ${interval.end}] interval, ${FrameType.TYPE_100MS} frame type" }
        val counter = storage.getMessageCountersAsync(bookId, sessionAlias, direction, FrameType.TYPE_100MS, interval)
            .await().asSequence().firstOrNull()

        cache.get(sessionAlias)?.let { group ->
            K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
            return group
        }

        if (counter == null) {
            K_LOGGER.info { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval" }
            return null
        }

        val shortInterval = Interval(counter.frameStart, counter.frameStart.plusMillis(FrameType.TYPE_100MS.millisInFrame))
        return seachSessionGorupByGroupedMessage(sessionAlias, shortInterval, bookId, cache).also {
            if (it == null) {
                K_LOGGER.warn { "Mapping between a session group and the '${sessionAlias}' alias isn't found by statistics, book: ${bookId.name}, interval: [${interval.start}, ${interval.end}] " }
            }
        }
    }

    private suspend fun seachSessionGorupByPage(
        sessionAlias: String,
        interval: Interval,
        bookId: BookId,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Strat searching '$sessionAlias' session alias in cradle in [${interval.start}, ${interval.end}] interval by page" }
        val pageInterval = storage.getPagesAsync(bookId, interval).await().asSequence()
            .map { pageInfo -> pageInfo.toInterval() }
            .filter { pageInterval ->
                storage.getSessionAliases(bookId, pageInterval).asSequence()
                    .any { alias -> alias == sessionAlias }
            }.firstOrNull()

        cache.get(sessionAlias)?.let { group ->
            K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
            return group
        }

        if (pageInterval == null) {
            K_LOGGER.info { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval" }
            return null
        }

        val targetInterval = intersection(interval, pageInterval)

        return seachSessionGorupByGroupedMessage(sessionAlias, targetInterval, bookId, cache).also {
            if (it == null) {
                K_LOGGER.warn { "Mapping between a session group and the '${sessionAlias}' alias isn't found by page, book: ${bookId.name}, interval: [${interval.start}, ${interval.end}] " }
            }
        }
    }

    private suspend fun seachSessionGorupByGroupedMessage(
        sessionAlias: String,
        shortInterval: Interval,
        bookId: BookId,
        cache: Cache<String, String>
    ): String? {
        K_LOGGER.debug { "Strat searching session group by '$sessionAlias' alias in cradle in (${shortInterval.start}, ${shortInterval.end}) interval" }
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
                    K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
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

    private suspend fun getSessionGroupSuspend(
        filter: MessageFilter
    ): String? = getSessionGroupSuspend(
        filter.bookId,
        filter.timestampFrom?.value,
        filter.timestampTo?.value,
        filter.sessionAlias,
        filter.direction
    )

    private suspend fun getSessionGroupSuspend(
        id: StoredMessageId
    ): String? = getSessionGroupSuspend(id.bookId, id.timestamp, id.timestamp, id.sessionAlias, id.direction)
}
