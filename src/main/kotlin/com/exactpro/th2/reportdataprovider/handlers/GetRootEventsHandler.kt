package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.reportdataprovider.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.getEventIdsSuspend
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

suspend fun getRootEvents(
    request: EventSearchRequest,
    manager: CradleManager,
    eventCache: EventCacheManager,
    timeout: Long
): List<Any> {
    val linker = manager.storage.testEventsMessagesLinker

    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            manager.storage.rootTestEvents.asFlow()
                .map { event ->
                    async {
                        event to (
                                (request.attachedMessageId?.let {
                                    linker.getEventIdsSuspend(StoredMessageId.fromString(it)).contains(event.id)
                                } ?: true)

                                        && (request.name?.let { event.name.toLowerCase().contains(it.toLowerCase()) }
                                    ?: true)
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
}
