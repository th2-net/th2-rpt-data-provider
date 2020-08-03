/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider.cache

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.Configuration
import com.exactpro.th2.reportdataprovider.entities.Event
import com.exactpro.th2.reportdataprovider.getEventSuspend
import com.exactpro.th2.reportdataprovider.getEventsSuspend
import com.exactpro.th2.reportdataprovider.unwrap
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant

class EventCacheManager(configuration: Configuration, private val cradleManager: CassandraCradleManager) {

    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }
    private val timeout = configuration.serverCacheTimeout.value.toLong()

    data class CachedEvent(val event: Event, val isBatched: Boolean, val cachedAt: Instant)

    private val cache: Cache<Path, CachedEvent> = manager.createCache(
        "events",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            Path::class.java,
            CachedEvent::class.java,
            ResourcePoolsBuilder.heap(configuration.eventCacheSize.value.toLong())
        ).build()
    )

    fun put(pathString: String, event: Event) {
        val path = Paths.get(pathString)

        if (!cache.containsKey(path)) {
            cache.put(path, CachedEvent(event, event.isBatched, Instant.now()))
        }
    }

    private fun validateAndReturn(key: Path): CachedEvent? {
        val cached = cache.get(key)

        return if (cached != null && (!cached.isBatched)
            && Duration.between(cached.cachedAt, Instant.now()).toMillis() > timeout
        ) {
            cache.remove(key)
            logger.debug { "event with path=$key is removed from cache" }
            null
        } else {
            cached
        }
    }

    fun get(pathString: String): Event? {
        return validateAndReturn(Paths.get(pathString))?.event
    }

    suspend fun getOrPut(pathString: String): Event? {
        val path = Paths.get(pathString)

        validateAndReturn(path)?.let { return it.event }

        logger.debug { "Event cache miss for path=$path" }
        return traverseEventChainRecursive(path, path)
    }

    private suspend fun saveSubtreeRecursive(
        events: Collection<StoredTestEventWithContent>,
        parent: StoredTestEventId?,
        path: Path
    ): Set<Pair<Path, Event>> {
        val filtered = events.filter { it.parentId == parent }.toSet()

        logger.debug {
            "${filtered.count()} items out of ${events.count()} are children of ${parent?.toString() ?: "<null>"} "
        }

        return withContext(Dispatchers.Default) {
            filtered
                .map {
                    async {
                        path.resolve(it.id.toString()) to Event(it, cradleManager,
                            events
                                .filter { event -> event.parentId == it.id }
                                .sortedBy { event -> event.startTimestamp.toEpochMilli() }
                                .map { event -> event.id.toString() },
                            true
                        )
                    }
                }
                .map { it.await() }
                .union(
                    filtered
                        .map {
                            async { saveSubtreeRecursive(events, it.id, path.resolve(it.id.toString())) }
                        }
                        .flatMap { it.await() }
                )
        }
    }

    private suspend fun traverseEventChainRecursive(path: Path, fullPath: Path): Event? {
        return withContext(Dispatchers.Default) {
            cradleManager.storage.getEventSuspend(StoredTestEventId(path.fileName.toString()))?.let {
                logger.debug { "found last non-batched parent (id=${it.id})" }

                val unwrappedChildren = cradleManager.storage.getEventsSuspend(it.id)
                    .flatMap { event -> event.unwrap() }

                System.gc()

                val directChildren = unwrappedChildren
                    .filter { unwrapped -> unwrapped.event.parentId == it.id }
                    .map { unwrapped -> unwrapped.event }

                val unbatchedChildren = saveSubtreeRecursive(
                    unwrappedChildren
                        .filter { unwrapped -> unwrapped.isBatched }
                        .map { unwrapped -> unwrapped.event },
                    it.id,
                    path
                ).toMap()

                cache.putAll(unbatchedChildren.mapValues { entry ->
                    CachedEvent(
                        entry.value,
                        entry.value.isBatched,
                        Instant.now()
                    )
                })

                if (unbatchedChildren.containsKey(fullPath)) {
                    logger.debug { "batched event id=${it.id} has been found (path=$fullPath)" }
                    return@withContext unbatchedChildren[fullPath]
                }

                if (it.id.toString() == fullPath.fileName.toString() && it.isSingle) {
                    logger.debug { "single event id=${it.id} has been found (path=$fullPath)" }
                    val event = Event(
                        it.unwrap().first().event,
                        cradleManager,

                        directChildren
                            .sortedBy { event -> event.startTimestamp?.toEpochMilli() ?: 0 }
                            .map { event -> event.id.toString() },
                        false
                    )

                    cache.put(
                        fullPath,
                        event.let { eventToCache -> CachedEvent(eventToCache, eventToCache.isBatched, Instant.now()) }
                    )

                    return@withContext event
                }

                logger.error { "no matching event (path=$fullPath) has been found" }
                return@withContext null
            }

            if (path.toString().isEmpty())
                null
            else
                traverseEventChainRecursive(path.parent, path)
        }
    }
}
