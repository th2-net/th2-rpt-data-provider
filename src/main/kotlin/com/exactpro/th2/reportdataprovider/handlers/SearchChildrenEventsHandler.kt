package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.th2.reportdataprovider.EventSearchRequest
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import java.nio.file.Paths

@Suppress("ConvertCallChainIntoSequence")
suspend fun searchChildrenEvents(
    request: EventSearchRequest,
    pathString: String,
    eventCache: EventCacheManager
): List<Any> {
    return withContext(Dispatchers.Default) {
        (eventCache.getOrPut(pathString)?.childrenIds ?: listOf<String>().asIterable())
            .map { id ->
                async {
                    val event = eventCache.getOrPut(Paths.get(pathString, id).toString())!!

                    event to (
                            (request.attachedMessageId?.let {
                                event.attachedMessageIds?.contains(request.attachedMessageId) ?: false
                            } ?: true)

                                    && (request.name?.let {
                                event.eventName.toLowerCase().contains(it.toLowerCase())
                            } ?: true)

                                    && (request.type?.let { event.type == it } ?: true)

                                    && (request.timestampFrom?.let { event.endTimestamp?.isAfter(it) ?: false }
                                ?: true)

                                    && (request.timestampTo?.let { event.startTimestamp.isBefore(it) }
                                ?: true)
                            )
                }
            }
            .map { it.await() }
            .filter { it.second }
            .sortedByDescending { it.first.startTimestamp.toEpochMilli() }
            .map {
                if (request.idsOnly) {
                    it.first.eventId
                } else {
                    it.first
                }
            }
            .toList()
    }
}
