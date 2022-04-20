/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.fasterxml.jackson.databind.ObjectMapper
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope.coroutineContext
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.io.IOException
import java.io.Writer
import java.time.Instant
import java.util.concurrent.Executors
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

        logger.debug { "cradle: $methodName is starting" }

        measureTimeMillis { result = lambda.invoke() }
            .also { logger.debug { "cradle: $methodName took ${it}ms" } }

        result
    }
}

data class Metrics(
    private val histogramTime: Histogram,
    private val gauge: Gauge
) {

    constructor(variableName: String, descriptionName: String, labels: List<String> = listOf()) : this(
        histogramTime = Histogram.build(
            "${variableName}_hist_time", "Time of $descriptionName"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .labelNames(*labels.toTypedArray())
            .register(),
        gauge = Gauge.build("${variableName}_gauge", "Quantity of $descriptionName using Gauge")
            .labelNames(*labels.toTypedArray())
            .register()
    )

    fun gaugeInc(labels: List<String> = listOf()) {
        gauge.labels(*labels.toTypedArray()).inc()
    }

    fun gaugeDec(labels: List<String> = listOf()) {
        gauge.labels(*labels.toTypedArray()).dec()
    }

    fun setDuration(amount: Double, labels: List<String> = listOf()) {
        histogramTime.labels(*labels.toTypedArray()).observe(
            System.currentTimeMillis() - amount
        )
    }

    fun remove(streamName: String) {
        gauge.remove(streamName)
        histogramTime.remove(streamName)
    }

    fun labels(streamName: String) {
        gauge.labels(streamName)
        histogramTime.labels(streamName)
    }

    fun startObserve(labels: List<String> = listOf()): Histogram.Timer {
        gauge.labels(*labels.toTypedArray()).inc()
        return histogramTime.labels(*labels.toTypedArray()).startTimer()
    }

    fun stopObserve(timer: Histogram.Timer, labels: List<String> = listOf()) {
        gauge.labels(*labels.toTypedArray()).dec()
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


fun StoredMessageId.convertToProto(): MessageID {
    return MessageID.newBuilder()
        .setSequence(index)
        .setDirection(cradleDirectionToGrpc(direction))
        .setConnectionId(ConnectionID.newBuilder().setSessionAlias(streamName))
        .build()
}
