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

package com.exactpro.th2.rptdataprovider.cache

import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.time.Duration
import java.time.Instant

class EventCache(private val timeout: Long, size: Long, private val eventProducer: EventProducer) {

    private val cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    data class CachedEvent(val event: BaseEventEntity, val isBatched: Boolean, val cachedAt: Instant)

    private val cache: Cache<String, CachedEvent> = cacheManager.createCache(
        "events",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            CachedEvent::class.java,
            ResourcePoolsBuilder.heap(size)
        ).build()
    )

    private fun put(id: String, event: BaseEventEntity) {
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

    fun get(id: String): BaseEventEntity? {
        return validateAndReturn(id)?.event
    }

    suspend fun getOrPut(id: String): BaseEventEntity {
        return validateAndReturn(id)?.event
            ?: eventProducer.fromId(ProviderEventId(id)).also {
                put(id, it)
                logger.debug { "Event cache miss for id=$id" }
            }
    }

    suspend fun getOrPutMany(ids: Set<String>): List<BaseEventEntity> {
        return mutableListOf<BaseEventEntity>().apply {
            val needRequest = mutableListOf<String>()
            ids.forEach { id ->
                validateAndReturn(id)?.event?.let {
                    add(it)
                } ?: needRequest.add(id)
            }

            eventProducer.fromIds(needRequest.map { ProviderEventId(it) }).forEach {
                add(it)
                put(it.id.toString(), it)
                logger.debug { "Event cache miss for id=${it.id}" }
            }
        }
    }
}
