/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.producers.MessageProducer

import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

class MessageCache(configuration: Configuration, private val messageProducer: MessageProducer) {
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
        return cache.get(id)
            ?: messageProducer.fromId(StoredMessageId.fromString(id)).also {
                logger.debug { "Message cache miss for id=$id" }
                put(id, it)
            }
    }

    suspend fun getOrPut(rawMessage: StoredMessage, parsingOff: Boolean = false): Message {
        return cache.get(rawMessage.id.toString())
            ?: messageProducer.fromRawMessage(rawMessage, parsingOff).also {
                logger.debug { "Message cache miss for id=${rawMessage.id}" }
                put(rawMessage.id.toString(), it)
            }
    }
}
