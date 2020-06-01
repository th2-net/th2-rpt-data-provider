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
import java.nio.file.Paths
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
        path: Path?
    ): Set<Pair<Path, Event>> {
        val newPathFragment = parent?.let { Paths.get(parent.toString()) } ?: Paths.get("")

        val currentPath = path?.resolve(newPathFragment) ?: newPathFragment

        val filtered = events.filter { it.parentId == parent }.toSet()

        logger.debug {
            "${filtered.count()} items out of ${events.count()} are children of ${parent?.toString() ?: "<null>"} "
        }

        return filtered
            .map {
                currentPath.resolve(Paths.get(it.toString())) to Event(it, cradleManager,
                    events.filter { event -> event.parentId === it.id }.map { event -> event.id.toString() }.toSet()
                )
            }
            .union(filtered.flatMap { saveSubtreeRecursive(events, it.id, currentPath) })
    }

    private fun traverseEventChainRecursive(path: Path, fullPath: Path): Event? {
        cradleManager.storage.getTestEvent(StoredTestEventId(path.fileName.toString()))?.let {
            logger.debug { "non-batched parent found (id=${it.id})" }

            val events = saveSubtreeRecursive(it.unwrap(), it.id, path.parent).toMap()

            logger.info { "Tree is traversed. ${events.count() + 1} events have been unwrapped. Updating the cache..." }

            cache.putAll(events)

            if (it.id.toString() == fullPath.fileName.toString()) {
                val event = Event(it.unwrap().first(), cradleManager, events.values
                    .filter { value -> value.parentEventId == it.id.toString() }
                    .map { value -> value.eventId }.toSet()
                )

                cache.put(fullPath, event)
                return event
            }

            events[fullPath]?.let { result -> return result }

            logger.error { "Tree is traversed, but no matching event (path=$fullPath) was found." }
            return null
        }

        return if (path.toString().isEmpty()) null
        else traverseEventChainRecursive(path.parent, path)
    }

    private fun StoredTestEventWrapper.unwrap(): Collection<StoredTestEventWithContent> {
        return try {
            if (this.isSingle) {
                logger.debug { "unwrapped: id=${this.id} is a single event" }
                Collections.singletonList(this.asSingle())
            } else {
                logger.debug { "unwrapped: id=${this.id} is a batch with ${this.asBatch().testEventsCount} items" }
                this.asBatch().testEvents
            }
        } catch (e: Exception) {
            logger.error(e) { "unable to unwrap test events (id=${this.id})" }
            Collections.emptyList()
        }
    }
}
