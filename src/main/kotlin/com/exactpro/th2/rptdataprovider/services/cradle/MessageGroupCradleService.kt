/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import mu.KotlinLogging
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
    }

    private val aliasToGroupCacheSize = configuration.aliasToGroupCacheSize.value.toLong()

    private val bookToCache = ConcurrentHashMap<String, Cache<String, String>>()


    override suspend fun getMessageBatches(
        filter: MessageFilter
    ): Iterable<StoredMessageBatch> =
        getSessionGroupSuspend(filter)?.let { group ->
            val groupedMessageFilter = filter.toGroupedMessageFilter(group).also {
                K_LOGGER.debug { "Start searching group batches by $it" }
            }
            storage.getGroupedMessageBatchesAsync(groupedMessageFilter).await().asIterable()
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

        } ?: listOf()

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

            K_LOGGER.debug { "Strat searching '$sessionAlias' session alias counters in cradle in [${interval.start}, ${interval.end}] interval, ${FrameType.TYPE_100MS} frame type" }
            val counter = storage.getMessageCountersAsync(bookId, sessionAlias, direction, FrameType.TYPE_100MS, interval)
                .await().asSequence().firstOrNull()

            cache.get(sessionAlias)?.let { group ->
                K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
                return@logTime group
            }

            if (counter == null) {
                K_LOGGER.info { "'$sessionAlias' session alias isn't in [${interval.start}, ${interval.end}] interval" }
                return@logTime null
            }

            val shortInterval = Interval(counter.frameStart, counter.frameStart.plusMillis(100))
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
                        return@logTime group
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

            cache.get(sessionAlias)?.let { group ->
                K_LOGGER.debug { "Another coroutine has dound '$sessionAlias' session alias to '$group' group pair" }
                return@logTime group
            } ?: error("Mapping between a session group and the '${sessionAlias}' alias isn't found, book: ${bookId.name}, [from: $from, to: $to]")

        }
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
