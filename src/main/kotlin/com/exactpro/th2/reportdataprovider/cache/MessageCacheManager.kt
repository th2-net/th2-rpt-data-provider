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

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.reportdataprovider.Configuration
import com.exactpro.th2.reportdataprovider.entities.Message
import com.exactpro.th2.reportdataprovider.getMessageSuspend
import com.exactpro.th2.reportdataprovider.getProcessedMessageSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

class MessageCacheManager(configuration: Configuration, private val cradleManager: CradleManager) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

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

    suspend fun getOrPut(id: String): Message {
        cache.get(id)?.let { return it }

        logger.debug { "Message cache miss for id=$id" }

        val message = withContext(Dispatchers.Default) {
            val storedMessageId = StoredMessageId.fromString(id)

            Message(
                cradleManager.storage.getProcessedMessageSuspend(storedMessageId),
                cradleManager.storage.getMessageSuspend(storedMessageId)
            )
        }

        cache.put(id, message)
        return message
    }

}
