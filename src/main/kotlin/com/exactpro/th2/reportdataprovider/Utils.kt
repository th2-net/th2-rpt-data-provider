package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.infra.grpc.Message
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import kotlinx.coroutines.flow.Flow
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

fun <T, R> Sequence<T>.optionalFilter(value: R?, filter: (R, Sequence<T>) -> Sequence<T>): Sequence<T> {
    return if (value == null) this else filter(value, this)
}

fun <T, R> Flow<T>.optionalFilter(value: R?, filter: (R, Flow<T>) -> Flow<T>): Flow<T> {
    return if (value == null) this else filter(value, this)
}

fun StoredMessage.getMessageType(): String {
    try {
        return Message.parseFrom(this.content).metadata.messageType
    } catch (e: Exception) {
        logger.error(e) { "unable to get message type (id=${this.id})" }
    }

    return "unknown"
}

public data class Unwrapped(val isBatched: Boolean, val event: StoredTestEventWithContent)

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

