package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.*
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
import com.exactpro.th2.reportdataprovider.entities.Message
import kotlinx.coroutines.*

@Suppress("ConvertCallChainIntoSequence")
suspend fun searchMessages(
    request: MessageSearchRequest,
    manager: CradleManager,
    messageCache: MessageCacheManager,
    timeout: Long
): List<Any> {
    return withContext(Dispatchers.Default) {
        withTimeout(timeout) {
            val linker = manager.storage.testEventsMessagesLinker

            (if (request.stream == null) {
                manager.storage.getMessagesSuspend(
                    StoredMessageFilterBuilder()
                        .let {
                            if (request.timestampFrom != null)
                                it.timestampFrom().isGreaterThanOrEqualTo(request.timestampFrom) else it
                        }
                        .let {
                            if (request.timestampTo != null)
                                it.timestampTo().isLessThanOrEqualTo(request.timestampTo) else it
                        }
                        .build()
                )
            } else {
                request.stream.flatMap { streamName ->
                    manager.storage.getMessagesSuspend(
                        StoredMessageFilterBuilder()
                            .streamName().isEqualTo(streamName)
                            .let {
                                if (request.timestampFrom != null)
                                    it.timestampFrom().isGreaterThanOrEqualTo(request.timestampFrom) else it
                            }
                            .let {
                                if (request.timestampTo != null)
                                    it.timestampTo().isLessThanOrEqualTo(request.timestampTo) else it
                            }
                            .build()
                    )
                }
            })
                .map { message ->

                    if (!isActive) {
                        throw Exception("pre-filter calculations were cancelled")
                    }

                    async {
                        message to (
                                (request.attachedEventId?.let {
                                    linker.getEventIdsSuspend(message.id)
                                        .contains(StoredTestEventId(it))
                                } ?: true)

                                        && (request.messageType?.contains(
                                    manager.storage.getProcessedMessageSuspend(message.id)?.getMessageType()
                                        ?: "unknown"
                                ) ?: true)
                                )
                    }
                }
                .map { it.await() }
                .filter { it.second }
                .sortedByDescending { it.first.timestamp?.toEpochMilli() ?: 0 }
                .map {

                    if (!isActive) {
                        throw Exception("result generation was cancelled")
                    }

                    async {
                        val event = it.first

                        if (request.idsOnly) {
                            event.id.toString()
                        } else {
                            messageCache.get(event.id.toString())
                                ?: Message(
                                    manager.storage.getProcessedMessageSuspend(
                                        event.id
                                    ), event
                                )
                                    .let { message ->
                                        messageCache.put(event.id.toString(), message)
                                        message
                                    }
                        }
                    }
                }
                .map { it.await() }
                .toList()
        }
    }
}
