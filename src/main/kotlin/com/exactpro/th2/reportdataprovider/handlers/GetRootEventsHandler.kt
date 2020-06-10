package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.reportdataprovider.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.optionalFilter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

suspend fun getRootEvents(
    request: EventSearchRequest,
    manager: CradleManager,
    eventCache: EventCacheManager
): List<Any> {
    val linker = manager.storage.testEventsMessagesLinker

    return withContext(Dispatchers.IO) {
        manager.storage.rootTestEvents
            .asSequence()
            .optionalFilter(request.attachedMessageId) { value, stream ->
                stream.filter { linker.getTestEventIdsByMessageId(StoredMessageId.fromString(value)).contains(it.id) }
            }
            .optionalFilter(request.name) { value, stream ->
                stream.filter { it.name?.contains(value) ?: false }
            }
            .optionalFilter(request.type) { value, stream ->
                stream.filter { it.type == value }
            }
            .optionalFilter(request.timestampFrom) { value, stream ->
                stream.filter { it.endTimestamp?.isAfter(value) ?: false }
            }
            .optionalFilter(request.timestampTo) { value, stream ->
                stream.filter { it.startTimestamp?.isBefore(value) ?: false }
            }
            .map {
                val id = it.id.toString()

                if (request.idsOnly) {
                    id
                } else {
                    eventCache.getOrPut(id)
                }
            }
            .filterNotNull()
            .toList()
    }
}
