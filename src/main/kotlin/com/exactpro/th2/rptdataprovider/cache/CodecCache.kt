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

import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

class CodecCache(configuration: Configuration) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    private val cache: Cache<String, Message> = manager.createCache(
        "codec",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            Message::class.java,
            ResourcePoolsBuilder.heap(configuration.codecCacheSize.value.toLong())
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
}

class CodecCacheBatches(configuration: Configuration) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    private val cache: Cache<String, ParsedMessageBatch> = manager.createCache(
        "codecBatch",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            ParsedMessageBatch::class.java,
            ResourcePoolsBuilder.heap(configuration.codecBatchesCacheSize.value.toLong())
        ).build()
    )

    fun put(id: String, message: ParsedMessageBatch) {
        if (!cache.containsKey(id)) {
            cache.put(id, message)
        }
    }

    fun get(id: String): ParsedMessageBatch? {
        return cache.get(id)
    }
}