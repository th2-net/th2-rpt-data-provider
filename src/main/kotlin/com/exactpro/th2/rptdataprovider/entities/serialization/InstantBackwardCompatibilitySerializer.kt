package com.exactpro.th2.rptdataprovider.entities.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import java.time.Instant

@Deprecated("used to keep backward compatibility with previous version of jackson. Should be removed when migrated to a normal format for instant")
object InstantBackwardCompatibilitySerializer : StdSerializer<Instant>(Instant::class.java) {
    private const val serialVersionUID: Long = 6738326988027583780L

    override fun serialize(value: Instant, gen: JsonGenerator, provider: SerializerProvider) {
        with(gen) {
            writeStartObject(value)
            writeField("epochSecond", value.epochSecond)
            writeField("nano", value.nano.toLong())
            writeEndObject()
        }
    }

    private fun JsonGenerator.writeField(name: String, value: Long) {
        writeFieldName(name)
        writeNumber(value)
    }
}