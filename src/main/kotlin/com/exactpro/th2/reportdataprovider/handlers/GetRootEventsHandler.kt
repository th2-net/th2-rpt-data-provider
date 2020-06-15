package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.reportdataprovider.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.getEventIdsSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext

@Suppress("ConvertCallChainIntoSequence")
suspend fun getRootEvents(
    request: EventSearchRequest,
    manager: CradleManager,
    eventCache: EventCacheManager
): List<Any> {
    val linker = manager.storage.testEventsMessagesLinker

    return withContext(Dispatchers.Default) {
        manager.storage.rootTestEvents
            .map { event ->
                async {
                    event to (
                            (request.attachedMessageId?.let {
                                linker.getEventIdsSuspend(StoredMessageId.fromString(it)).contains(event.id)
                            } ?: true)

                                    && (request.name?.let { event.name.contains(it) } ?: true)
                                    && (request.type?.let { event.type == it } ?: true)

                                    && (request.timestampFrom?.let { event.endTimestamp?.isAfter(it) ?: false }
                                ?: true)

                                    && (request.timestampTo?.let { event.startTimestamp?.isBefore(it) ?: false }
                                ?: true)
                            )
                }
            }
            .map { it.await() }
            .filter { it.second }
            .sortedByDescending { it.first.startTimestamp?.toEpochMilli() ?: 0 }
            .map {
                async {
                    val id = it.first.id.toString()

                    if (request.idsOnly) {
                        id
                    } else {
                        eventCache.getOrPut(id)
                    }
                }
            }
            .mapNotNull { it.await() }
            .toList()
    }
}
