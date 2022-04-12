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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.internal.NonCachedMessageTypes
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import kotlinx.coroutines.InternalCoroutinesApi
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

class MessageCache(configuration: Configuration, private val messageProducer: MessageProducer) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    companion object {
        private val nonCachedTypes = NonCachedMessageTypes.values().map { it.value }
    }

    private val cache: Cache<String, Message> = manager.createCache(
        "messages",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            Message::class.java,
            ResourcePoolsBuilder.heap(configuration.messageCacheSize.value.toLong())
        ).build()
    )

    fun put(id: String, message: Message) {
        if (!cache.containsKey(id)) {
            cache.put(id, message)
        }
    }

    fun get(id: String): Message? {
        return cache.get(id)
    }

    @InternalCoroutinesApi
    suspend fun getOrPut(id: String): Message {
        return messageProducer.fromId(StoredMessageId.fromString(id)).also {
                logger.debug { "Message cache miss for id=$id" }

                val type = it.parsedMessageGroup?.get(0)?.messageType

                if (!nonCachedTypes.contains(type)) {
                    put(id, it)
                }
            }
    }
}
