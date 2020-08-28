/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.reportdataprovider.Configuration
import com.exactpro.th2.reportdataprovider.entities.Event
import com.exactpro.th2.reportdataprovider.entities.ProviderEventId
import com.exactpro.th2.reportdataprovider.getEventSuspend
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.time.Duration
import java.time.Instant

class EventCacheManager(configuration: Configuration, private val cradleManager: CassandraCradleManager) {

    private val cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }
    private val timeout = configuration.serverCacheTimeout.value.toLong()

    data class CachedEvent(val event: Event, val isBatched: Boolean, val cachedAt: Instant)

    private val cache: Cache<String, CachedEvent> = cacheManager.createCache(
        "events",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            CachedEvent::class.java,
            ResourcePoolsBuilder.heap(configuration.eventCacheSize.value.toLong())
        ).build()
    )

    fun put(id: String, event: Event) {
        if (!cache.containsKey(id)) {
            cache.put(id, CachedEvent(event, event.isBatched, Instant.now()))
        }
    }

    private fun validateAndReturn(id: String): CachedEvent? {
        val cached = cache.get(id)

        return if (cached != null && (!cached.isBatched)
            && Duration.between(cached.cachedAt, Instant.now()).toMillis() > timeout
        ) {
            cache.remove(id)
            logger.debug { "event with id=$id is removed from cache" }
            null
        } else {
            cached
        }
    }

    fun get(id: String): Event? {
        return validateAndReturn(id)?.event
    }

    suspend fun getOrPut(id: String): Event {
        validateAndReturn(id)?.let { return it.event }
        logger.debug { "Event cache miss for id=$id" }

        val parsedId = ProviderEventId(id)
        val batch = parsedId.batchId?.let { cradleManager.storage.getEventSuspend(it)?.asBatch() }

        if (parsedId.batchId != null && batch == null) {
            logger.error { "unable to find batch with id '${parsedId.batchId}' - this is a bug" }
        }

        val storedEvent =
            batch?.getTestEvent(parsedId.eventId) ?: cradleManager.storage.getEventSuspend(parsedId.eventId)?.asSingle()

        if (storedEvent == null) {
            logger.error { "unable to find event with id '${parsedId.eventId}'" }
            throw IllegalArgumentException("${parsedId.eventId} is not a valid id")
        }

        return Event(storedEvent, cradleManager, batch?.id?.toString())
    }
}
