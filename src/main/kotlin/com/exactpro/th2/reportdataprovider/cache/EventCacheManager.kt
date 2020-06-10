package com.exactpro.th2.reportdataprovider.cache

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.Configuration
import com.exactpro.th2.reportdataprovider.entities.Event
import com.exactpro.th2.reportdataprovider.unwrap
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.nio.file.Path
import java.nio.file.Paths

class EventCacheManager(configuration: Configuration, private val cradleManager: CassandraCradleManager) {
    private val manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)
    private val logger = KotlinLogging.logger { }

    private val cache: Cache<Path, Event> = manager.createCache(
        "events",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
            Path::class.java,
            Event::class.java,
            ResourcePoolsBuilder.heap(configuration.eventCacheSize.value.toLong())
        ).build()
    )

    fun put(pathString: String, event: Event) {
        val path = Paths.get(pathString)

        if (!cache.containsKey(path)) {
            cache.put(path, event)
        }
    }

    fun get(pathString: String): Event? {
        return cache.get(Paths.get(pathString))
    }

    fun getOrPut(pathString: String): Event? {
        val path = Paths.get(pathString)

        cache.get(path)?.let { return it }

        logger.debug { "Event cache miss for path=$path" }
        return traverseEventChainRecursive(path, path)
    }

    private fun saveSubtreeRecursive(
        events: Collection<StoredTestEventWithContent>,
        parent: StoredTestEventId?,
        path: Path
    ): Set<Pair<Path, Event>> {
        val filtered = events.filter { it.parentId == parent }.toSet()

        logger.debug {
            "${filtered.count()} items out of ${events.count()} are children of ${parent?.toString() ?: "<null>"} "
        }

        return filtered
            .map {
                path.resolve(it.id.toString()) to Event(it, cradleManager,
                    events
                        .filter { event -> event.parentId == it.id }
                        .sortedBy { event -> event.startTimestamp.toEpochMilli() }
                        .map { event -> event.id.toString() }
                )
            }
            .union(filtered.flatMap { saveSubtreeRecursive(events, it.id, path.resolve(it.id.toString())) })
    }

    private fun traverseEventChainRecursive(path: Path, fullPath: Path): Event? {
        cradleManager.storage.getTestEvent(StoredTestEventId(path.fileName.toString()))?.let {
            logger.debug { "found last non-batched parent (id=${it.id})" }

            val unwrappedChildren = cradleManager.storage.getTestEvents(it.id)
                .flatMap { event -> event.unwrap() }

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

            cache.putAll(unbatchedChildren)

            if (unbatchedChildren.containsKey(fullPath)) {
                logger.debug { "batched event id=${it.id} has been found (path=$fullPath)" }
                return unbatchedChildren[fullPath]
            }

            if (it.id.toString() == fullPath.fileName.toString() && it.isSingle) {
                logger.debug { "single event id=${it.id} has been found (path=$fullPath)" }
                val event = Event(
                    it.unwrap().first().event,
                    cradleManager,

                    directChildren
                        .sortedBy { event -> event.startTimestamp?.toEpochMilli() ?: 0 }
                        .map { event -> event.id.toString() }
                )

                cache.put(fullPath, event)
                return event
            }

            logger.error { "no matching event (path=$fullPath) has been found" }
            return null
        }

        return if (path.toString().isEmpty()) null
        else traverseEventChainRecursive(path.parent, path)
    }
}
