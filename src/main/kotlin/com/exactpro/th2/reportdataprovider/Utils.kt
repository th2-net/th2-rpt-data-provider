package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.cradle.testevents.TestEventsMessagesLinker
import com.exactpro.th2.infra.grpc.Message
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import java.util.*

private val logger = KotlinLogging.logger { }

class InstantSerializer : JsonSerializer<Instant>() {
    override fun serialize(value: Instant?, generator: JsonGenerator?, serializers: SerializerProvider?) {
        generator?.writeString(formatter.format(value))
    }
}

class InstantDeserializer : JsonDeserializer<Instant>() {
    override fun deserialize(parser: JsonParser?, context: DeserializationContext?): Instant {
        return Instant.from(formatter.parse(parser?.text))
    }
}

fun String.toInstant(): Instant? {
    return try {
        Instant.from(formatter.parse(this))
    } catch (e: Exception) {
        logger.error(e) { "unable to parse instant from string '$this'" }
        null
    }
}

fun StoredMessage.getMessageType(): String {
    try {
        return Message.parseFrom(this.content).metadata.messageType
    } catch (e: Exception) {
        logger.error(e) { "unable to get message type (id=${this.id})" }
    }

    return "unknown"
}

data class Unwrapped(val isBatched: Boolean, val event: StoredTestEventWithContent)

fun StoredTestEventWrapper.unwrap(): Collection<Unwrapped> {
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

suspend fun ObjectMapper.asStringSuspend(data: Any?): String {
    val mapper = this

    return withContext(Dispatchers.IO) {
        mapper.writeValueAsString(data)
    }
}

suspend fun CradleStorage.getMessagesSuspend(filter: StoredMessageFilter): Iterable<StoredMessage> {
    val storage = this

    return withContext(Dispatchers.IO) {
        storage.getMessages(filter)
    }
}

suspend fun CradleStorage.getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
    val storage = this

    return withContext(Dispatchers.IO) {
        storage.getProcessedMessage(id)
    }
}

suspend fun CradleStorage.getMessageSuspend(id: StoredMessageId): StoredMessage? {
    val storage = this

    return withContext(Dispatchers.IO) {
        storage.getMessage(id)
    }
}

suspend fun CradleStorage.getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
    val storage = this

    return withContext(Dispatchers.IO) {
        storage.getTestEvent(id)
    }
}

suspend fun CradleStorage.getEventsSuspend(parentId: StoredTestEventId): Iterable<StoredTestEventWrapper> {
    val storage = this

    return withContext(Dispatchers.IO) {
        storage.getTestEvents(parentId)
    }
}

suspend fun TestEventsMessagesLinker.getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
    val linker = this

    return withContext(Dispatchers.IO) {
        linker.getTestEventIdsByMessageId(id)
    }
}

