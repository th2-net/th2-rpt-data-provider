/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.rptdataprovider


import com.exactpro.cradle.filters.ComparisonOperation
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.*
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.fasterxml.jackson.databind.ObjectMapper
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope.coroutineContext
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.io.IOException
import java.time.Instant
import kotlin.system.measureTimeMillis

private val logger = KotlinLogging.logger { }

suspend fun ObjectMapper.asStringSuspend(data: Any?): String {
    val mapper = this

    return withContext(Dispatchers.IO) {
        mapper.writeValueAsString(data)
    }
}

fun MessageFilter.convertToString(): String {
    val filter = this

    return "(limit=${filter.limit} " +
            "direction=${filter.direction?.name} " +
            "timestampFrom=${filter.timestampFrom?.value} " +
            "timestampTo=${filter.timestampTo?.value} " +
            "sessionAlias=${filter.sessionAlias} " +
            "bookId=${filter.bookId} " +
            "pageId=${filter.pageId}" +
            "limit=${filter.limit}" +
            "sequence=${filter.sequence}" +
            "order=${filter.order}"
}

fun MessageFilter.toGroupedMessageFilter(group: String?): GroupedMessageFilter = GroupedMessageFilter.builder().also { builder ->
     builder.bookId(bookId)
        .pageId(pageId)
        .groupName(group)
        .order(order)

    when(timestampFrom?.operation) {
        null -> { /* do noting */ }
        ComparisonOperation.GREATER -> builder.timestampFrom().isGreaterThan(timestampFrom.value)
        ComparisonOperation.GREATER_OR_EQUALS -> builder.timestampFrom().isGreaterThanOrEqualTo(timestampFrom.value)
        else -> error("The '${timestampFrom.operation}' operation isn't supported")
    }
    when(timestampTo?.operation) {
        null -> { /* do noting */ }
        ComparisonOperation.LESS -> builder.timestampTo().isLessThan(timestampTo.value)
        ComparisonOperation.LESS_OR_EQUALS -> builder.timestampTo().isLessThanOrEqualTo(timestampTo.value)
        else -> error("The '${timestampTo.operation}' operation isn't supported")
    }
}.build()

@OptIn(DelicateCoroutinesApi::class)
suspend fun <T> logTime(methodName: String, lambda: suspend () -> T): T? {
    return withContext(coroutineContext) {
        var result: T?

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

@OptIn(DelicateCoroutinesApi::class)
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

fun StoredTestEventBatch.tryToGetTestEvents(parentEventId: StoredTestEventId? = null): Collection<BatchedStoredTestEvent> {
    return try {
        this.testEvents?.let { events ->
            if (parentEventId != null) {
                events.filter { it.parentId == parentEventId }
            } else {
                events
            }
        }?: emptyList()
    } catch (e: IOException) {
        logger.error(e) { "unexpected IO exception while trying to parse an event batch - contents were ignored" }
        emptyList()
    }
}


fun StoredMessageId.convertToProto(): MessageID = MessageID.newBuilder()
    .setSequence(this.sequence)
    .setDirection(cradleDirectionToGrpc(direction))
    .setConnectionId(ConnectionID.newBuilder().setSessionAlias(this.sessionAlias))
    .setTimestamp(timestamp.toTimestamp())
    .setBookName(bookId.name)
    .build()

fun StoredMessageId.convertToTransport(): MessageId {
    return MessageId.builder()
        .setSequence(this.sequence)
        .setDirection(cradleDirectionToTransport(direction))
        .setSessionAlias(this.sessionAlias)
        .setTimestamp(timestamp)
        .build()
}