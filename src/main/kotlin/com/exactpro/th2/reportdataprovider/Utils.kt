/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

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
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.system.measureTimeMillis

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
        logTime("getMessages (filter=${filter.convertToString()})") {
            storage.getMessages(filter)
        }
    }
}

suspend fun CradleStorage.getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
    val storage = this

    return withContext(Dispatchers.IO) {
        logTime("getProcessedMessage (id=$id)") {
            storage.getProcessedMessage(id)
        }
    }
}

suspend fun CradleStorage.getMessageSuspend(id: StoredMessageId): StoredMessage? {
    val storage = this

    return withContext(Dispatchers.IO) {
        logTime("getMessage (id=$id)") {
            storage.getMessage(id)
        }
    }
}

suspend fun CradleStorage.getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
    val storage = this

    return withContext(Dispatchers.IO) {
        logTime("getTestEvent (id=$id)") {
            storage.getTestEvent(id)
        }
    }
}

suspend fun CradleStorage.getEventsSuspend(parentId: StoredTestEventId): Iterable<StoredTestEventWrapper> {
    val storage = this

    return withContext(Dispatchers.IO) {
        logTime("getTestEvents (parentId=$parentId)") {
            storage.getTestEvents(parentId)
        }
    }
}

suspend fun TestEventsMessagesLinker.getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
    val linker = this

    return withContext(Dispatchers.IO) {
        logTime("getTestEventIdsByMessageId (id=$id)") {
            linker.getTestEventIdsByMessageId(id)
        }
    }
}

suspend fun <T> logTime(methodName: String, lambda: () -> T): T {
    var result: T? = null

    measureTimeMillis { result = lambda.invoke() }
        .also { logger.debug { "cradle: $methodName took ${it}ms" } }

    return result!!
}

fun StoredMessageFilter.convertToString(): String {
    val filter = this

    return "(limit=${filter.limit} " +
            "direction=${filter.direction?.value} " +
            "timestampFrom=${filter.timestampFrom?.value} " +
            "timestampTo=${filter.timestampTo?.value} " +
            "stream=${filter.streamName?.value} " +
            "timestampDiff=${if (filter.timestampFrom != null && filter.timestampTo != null) {
                Duration.between(filter.timestampFrom.value, filter.timestampTo.value)
            } else null})"
}

