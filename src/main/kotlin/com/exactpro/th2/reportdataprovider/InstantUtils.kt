package com.exactpro.th2.reportdataprovider

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import mu.KotlinLogging
import java.time.Instant

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
        KotlinLogging.logger {}.error { "unable to parse instant from string '$this'" }
        null
    }
}
