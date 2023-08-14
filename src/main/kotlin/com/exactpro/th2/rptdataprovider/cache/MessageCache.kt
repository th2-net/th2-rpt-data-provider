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

package com.exactpro.th2.rptdataprovider.cache

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import kotlinx.coroutines.InternalCoroutinesApi
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

//FIXME remove it later
class MessageCache<RM, PM>(configuration: Configuration, private val messageProducer: MessageProducer<RM, PM>) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)

    @Suppress("UNCHECKED_CAST")
    private val cache: Cache<String, Message<RM, PM>> = manager.createCache(
        "messages",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            Message::class.java,
            ResourcePoolsBuilder.heap(configuration.messageCacheSize.value.toLong())
        ).build()
    ) as Cache<String, Message<RM, PM>>

    private fun put(id: String, message: Message<RM, PM>) {
        if (!cache.containsKey(id)) {
            cache.put(id, message)
        }
    }

    private fun get(id: String): Message<RM, PM>? {
        return cache.get(id)
    }

    @InternalCoroutinesApi
    suspend fun getOrPut(id: String): Message<RM, PM> {
        return messageProducer.fromId(StoredMessageId.fromString(id))
    }
}
