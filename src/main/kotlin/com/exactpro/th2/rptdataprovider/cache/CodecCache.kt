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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder

data class CodecCacheData private constructor(
    val id: String,
    private val channel: Channel<Message?>,
    private val result: Deferred<Message?>
) {
    var messageIsSend: Boolean = false
    private set

    companion object {
        suspend fun build(id: String): CodecCacheData {
            return coroutineScope {
                val messageChannel = Channel<Message?>(0)
                CodecCacheData(id, messageChannel, async {
                    messageChannel.receive()
                })
            }
        }
    }

    suspend fun sendMessage(message: Message?) {
        messageIsSend = true
        coroutineScope {
            if (result.isActive) {
                launch {
                    channel.send(message)
                }
            }
        }
    }

    suspend fun get(): Message? {
        return result.await()
    }
}

class CodecCache(configuration: Configuration) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    private val cache: Cache<String, CodecCacheData> = manager.createCache(
        "codec",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String::class.java,
            CodecCacheData::class.java,
            ResourcePoolsBuilder.heap(configuration.codecCacheSize.value.toLong())
        ).build()
    )

    fun put(id: String, message: CodecCacheData) {
        if (!cache.containsKey(id)) {
            cache.put(id, message)
        }
    }

    suspend fun get(id: String): Message? {
        return cache.get(id).get()
    }
}
