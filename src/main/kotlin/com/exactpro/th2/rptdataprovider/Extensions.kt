/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider

import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.io.Writer
import java.time.Instant
import kotlin.coroutines.coroutineContext
import kotlin.system.measureTimeMillis

private val logger = KotlinLogging.logger { }

suspend fun ObjectMapper.asStringSuspend(data: Any?): String {
    val mapper = this

    return withContext(Dispatchers.IO) {
        mapper.writeValueAsString(data)
    }
}

fun StoredMessageFilter.convertToString(): String {
    val filter = this

    return "(limit=${filter.limit} " +
            "direction=${filter.direction?.value} " +
            "timestampFrom=${filter.timestampFrom?.value} " +
            "timestampTo=${filter.timestampTo?.value} " +
            "stream=${filter.streamName?.value} " +
            "indexValue=${filter.index?.value} " +
            "indexOperation=${filter.index?.operation?.name}"
}

suspend fun <T> logTime(methodName: String, lambda: suspend () -> T): T? {
    return withContext(coroutineContext) {
        var result: T? = null

        measureTimeMillis { result = lambda.invoke() }
            .also { logger.debug { "cradle: $methodName took ${it}ms" } }

        result
    }
}


suspend fun Writer.eventWrite(event: SseEvent) {
    withContext(Dispatchers.Default) {
        if (event.event != null) {
            write("event: ${event.event}\n")
        }

        for (dataLine in event.data.lines()) {
            write("data: $dataLine\n")
        }

        if (event.id != null) {
            write("id: ${event.id}\n")
        }
        write("\n")
        flush()
    }
}

suspend fun Writer.asyncClose() {
    withContext(Dispatchers.IO) {
        close()
    }
}

fun Instant.min(other: Instant): Instant {
    return if (this.isBefore(other)) {
        this
    } else {
        other
    }
}


fun Instant.isBeforeOrEqual(other: Instant): Boolean {
    return this.isBefore(other) || this == other
}

fun Instant.isAfterOrEqual(other: Instant): Boolean {
    return this.isAfter(other) || this == other
}

