package com.exactpro.th2.reportdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.reportdataprovider.MessageSearchRequest
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
import com.exactpro.th2.reportdataprovider.entities.Message
import com.exactpro.th2.reportdataprovider.getMessageType
import com.exactpro.th2.reportdataprovider.optionalFilter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

suspend fun searchMessages(
    request: MessageSearchRequest,
    manager: CradleManager,
    messageCache: MessageCacheManager
): List<Any> {
    return withContext(Dispatchers.IO) {
        val linker = manager.storage.testEventsMessagesLinker

        manager.storage.getMessages(
            StoredMessageFilterBuilder()
                .let {
                    if (request.stream != null)
                        it.streamName().isEqualTo(request.stream.first()) else it
                }
                .let {
                    if (request.timestampFrom != null)
                        it.timestampFrom().isGreaterThanOrEqualTo(request.timestampFrom) else it
                }
                .let {
                    if (request.timestampTo != null)
                        it.timestampTo().isLessThanOrEqualTo(request.timestampTo) else it
                }
                .build()
        ).asSequence<StoredMessage>()
            .optionalFilter(request.attachedEventId) { value, stream ->
                stream.filter {
                    linker.getTestEventIdsByMessageId(it.id)
                        .contains(StoredTestEventId(value))

                }
            }
            .optionalFilter(request.messageType) { value, stream ->
                stream.filter {
                    value.contains(
                        manager.storage.getProcessedMessage(it.id)?.getMessageType()
                            ?: "unknown"
                    )
                }
            }
            .map {
                if (request.idsOnly) {
                    it.id.toString()
                } else {
                    messageCache.get(it.id.toString())
                        ?: Message(
                            manager.storage.getProcessedMessage(
                                it.id
                            ), it
                        )
                            .let { message ->
                                messageCache.put(it.id.toString(), message)
                                message
                            }
                }
            }
            .toList()
    }
}
