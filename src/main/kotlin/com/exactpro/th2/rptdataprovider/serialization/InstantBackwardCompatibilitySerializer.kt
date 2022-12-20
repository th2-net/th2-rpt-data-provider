/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.exactpro.th2.rptdataprovider.serialization

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