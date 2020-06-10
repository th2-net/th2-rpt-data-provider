package com.exactpro.th2.reportdataprovider.cache

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.reportdataprovider.Configuration
import com.exactpro.th2.reportdataprovider.entities.Message
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

    fun getOrPut(id: String): Message {
        cache.get(id)?.let { return it }

        logger.debug { "Message cache miss for id=$id" }
        val storedMessageId = StoredMessageId.fromString(id)

        val message = Message(
            cradleManager.storage.getProcessedMessage(storedMessageId),
            cradleManager.storage.getMessage(storedMessageId)
        )

        cache.put(id, message)
        return message
    }

}
