package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import mu.KotlinLogging
import org.ehcache.Cache
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import java.nio.file.Path
import java.util.*

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

    fun getEvent(path: Path): Event? {
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
                    events.filter { event -> event.parentId == it.id }.map { event -> event.id.toString() }.toSet()
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
                        .map { event -> event.id.toString() }
                        .toSet()
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

    private data class Unwrapped(val isBatched: Boolean, val event: StoredTestEventWithContent)

    private fun StoredTestEventWrapper.unwrap(): Collection<Unwrapped> {
        return try {
            if (this.isSingle) {
                logger.debug { "unwrapped: id=${this.id} is a single event" }
                Collections.singletonList(Unwrapped(false, this.asSingle()))
            } else {
                logger.debug { "unwrapped: id=${this.id} is a batch with ${this.asBatch().testEventsCount} items" }
                this.asBatch().testEvents.map { Unwrapped(true, it) }
            }
        } catch (e: Exception) {
            logger.error(e) { "unable to unwrap test events (id=${this.id})" }
            Collections.emptyList()
        }
    }
}
