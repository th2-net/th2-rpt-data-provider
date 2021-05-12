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
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.services.rabbitmq.BatchRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.Timestamp
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.whileSelect
import mu.KotlinLogging
import java.io.IOException
import java.io.Writer
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
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

data class Metrics(
    private val histogramTime: Histogram,
    private val gauge: Gauge
) {

    constructor(variableName: String, descriptionName: String) : this(
        histogramTime = Histogram.build(
            "${variableName}_hist_time", "Time of $descriptionName"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .register(),
        gauge = Gauge.build(
            "${variableName}_gauge", "Quantity of $descriptionName using Gauge"
        ).register()
    )

    fun startObserve(): Histogram.Timer {
        gauge.inc()
        return histogramTime.startTimer()
    }

    fun stopObserve(timer: Histogram.Timer) {
        gauge.dec()
        timer.observeDuration()
    }
}

suspend fun <T> logMetrics(metrics: Metrics, lambda: suspend () -> T): T? {
    return withContext(coroutineContext) {
        val timer = metrics.startObserve()
        try {
            lambda.invoke()
        } finally {
            metrics.stopObserve(timer)
        }
    }
}

private val writerDispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

suspend fun Writer.eventWrite(event: SseEvent) {
    withContext(writerDispatcher) {
        if (event.event != null) {
            write("event: ${event.event}\n")
        }

        for (dataLine in event.data.lines()) {
            write("data: $dataLine\n")
        }

        if (event.metadata != null) {
            write("id: ${event.metadata}\n")
        }

        write("\n")
        flush()
    }
}

suspend fun Writer.closeWriter() {
    withContext(writerDispatcher) {
        close()
    }
}

fun minInstant(first: Instant, second: Instant): Instant {
    return if (first.isBefore(second)) {
        first
    } else {
        second
    }
}

fun maxInstant(first: Instant, second: Instant): Instant {
    return if (first.isAfter(second)) {
        first
    } else {
        second
    }
}


fun Instant.isBeforeOrEqual(other: Instant): Boolean {
    return this.isBefore(other) || this == other
}

fun Instant.isAfterOrEqual(other: Instant): Boolean {
    return this.isAfter(other) || this == other
}


suspend fun <E> ReceiveChannel<E>.receiveAvailable(): List<E> {
    val allMessages = mutableListOf<E>()
    allMessages.add(receive())
    var next = poll()
    while (next != null) {
        allMessages.add(next)
        next = poll()
    }
    return allMessages
}


fun StoredTestEventMetadata.tryToGetTestEvents(parentEventId: StoredTestEventId? = null): Collection<BatchedStoredTestEventMetadata>? {
    return try {
        this.batchMetadata?.testEvents?.let { events ->
            if (parentEventId != null) {
                events.filter { it.parentId == parentEventId }
            } else {
                events
            }
        }
    } catch (e: IOException) {
        logger.error(e) { }
        null
    }
}


@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@InternalCoroutinesApi
fun ReceiveChannel<BatchRequest>.chunked(size: Int, time: Long, capacity: Int = 1000) =
    GlobalScope.produce<List<BatchRequest>>(capacity = capacity, onCompletion = consumes()) {
        while (true) {
            val chunk = ArrayList<BatchRequest>()
            val ticker = ticker(time)
            var messageCount = 0
            try {
                whileSelect {
                    ticker.onReceive {
                        false
                    }
                    this@chunked.onReceive {
                        chunk += it
                        messageCount += it.batch.messageCount
                        messageCount < size
                    }
                }
            } catch (e: ClosedReceiveChannelException) {
                return@produce
            } finally {
                ticker.cancel()
                if (chunk.isNotEmpty()) send(chunk)
            }
        }
    }

@InternalCoroutinesApi
@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
fun <T> Flow<T>.chunked(size: Int, duration: Duration): Flow<List<T>> {
    return flow {
        coroutineScope {
            val buffer = ArrayList<T>(size)
            val ticker = ticker(duration.toMillis())
            try {
                val upstreamValues = produce { collect { send(it) } }

                whileSelect {
                    ticker.onReceive {
                        false
                    }
                    upstreamValues.onReceive {
                        buffer += it
                        buffer.size < size
                    }
                }

                if (buffer.isNotEmpty()) {
                    emit(buffer.toList())
                    buffer.clear()
                }

            } catch (e: ClosedReceiveChannelException) {
                return@coroutineScope
            } finally {
                if (buffer.isNotEmpty()) emit(buffer.toList())
                ticker.cancel()
            }
        }
    }
}


fun Instant.convertToProto(): Timestamp {
    return Timestamp.newBuilder()
        .setNanos(this.nano)
        .setSeconds(this.epochSecond)
        .build()
}
