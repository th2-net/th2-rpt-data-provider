/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.th2.rptdataprovider.Context
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue
import java.util.concurrent.atomic.AtomicLong

data class PipelineStatusSnapshot(
    val startTime: Long,
    val processingTime: Long,
    val merged: Long,
    val returned: Long,
    val counters: Map<String, PipelineStreamCounters>
)

data class Counter(@JsonIgnore val streamName: String?, @JsonIgnore val metricName: String) {
    @JsonIgnore
    val name = "$metricName${if (streamName != null) "_${streamName}" else ""}"
        .replace("-", "_")

    @JsonIgnore
    private val counter: io.prometheus.client.Counter =
        io.prometheus.client.Counter.build(name, metricName)
            .register()

    @JsonValue
    val baseCounter: AtomicLong = AtomicLong(0)

    fun addAndGet(value: Long): Long {
        counter.inc(value.toDouble())
        return baseCounter.addAndGet(value)
    }

    fun incrementAndGet(): Long {
        counter.inc()
        return baseCounter.incrementAndGet()
    }

    fun get(): Long {
        return baseCounter.get()
    }
}

data class PipelineStreamCounters(
    @JsonIgnore
    val streamName: String,
    val fetched: Counter = Counter(streamName, "fetched"),
    val fetchedBytes: Counter = Counter(streamName, "fetchedBytes"),
    val fetchedBatches: Counter = Counter(streamName, "fetchedBatches"),
    val parsePrepared: Counter = Counter(streamName, "parsePrepared"),
    val parseRequested: Counter = Counter(streamName, "parseRequested"),
    val parseReceivedTotal: Counter = Counter(streamName, "parseReceivedTotal"),
    val parseReceivedFailed: Counter = Counter(streamName, "parseReceivedFailed"),
    val filterTotal: Counter = Counter(streamName, "filterTotal"),
    val filterDiscarded: Counter = Counter(streamName, "filterDiscarded"),
    val filterAccepted: Counter = Counter(streamName, "filterAccepted"),

    val fetchedStart: Counter = Counter(streamName, "fetchedStart"),
    val fetchedEnd: Counter = Counter(streamName, "fetchedEnd"),
    val fetchedSendDownstream: Counter = Counter(streamName, "fetchedSendDownstream"),

    val convertStart: Counter = Counter(streamName, "convertStart"),
    val convertEnd: Counter = Counter(streamName, "convertEnd"),
    val convertSendDownstream: Counter = Counter(streamName, "convertSendDownstream"),

    val decodeStart: Counter = Counter(streamName, "decodeStart"),
    val decodeEnd: Counter = Counter(streamName, "decodeEnd"),
    val decodeSendDownstream: Counter = Counter(streamName, "decodeSendDownstream"),

    val unpackStart: Counter = Counter(streamName, "unpackStart"),
    val unpackEnd: Counter = Counter(streamName, "unpackEnd"),
    val unpackSendDownstream: Counter = Counter(streamName, "unpackSendDownstream"),

    val filterStart: Counter = Counter(streamName, "filterStart"),
    val filterEnd: Counter = Counter(streamName, "filterEnd"),
    val filterSendDownstream: Counter = Counter(streamName, "filterSendDownstream")
)

class PipelineStatus(context: Context) {

    private val processingStartTimestamp: Long = System.currentTimeMillis()
    private val sendPipelineStatus = context.configuration.sendPipelineStatus.value.toBoolean()

    val streams: MutableMap<String, PipelineStreamCounters> = mutableMapOf()

    var merged: Counter = Counter(null, "merged")
    var sended: Counter = Counter(null, "sended")

    fun addStream(streamName: String) {
        if (sendPipelineStatus) {
            this.streams[streamName] = PipelineStreamCounters(streamName)
        }
    }

    fun fetchedStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedStart?.addAndGet(count)
    }

    fun fetchedEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedEnd?.addAndGet(count)
    }

    fun fetchedSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedSendDownstream?.addAndGet(count)
    }

    fun convertStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.convertStart?.addAndGet(count)
    }

    fun convertEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.convertEnd?.addAndGet(count)
    }

    fun convertSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.convertSendDownstream?.addAndGet(count)
    }

    fun decodeStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.decodeStart?.addAndGet(count)
    }


    fun decodeEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.decodeEnd?.addAndGet(count)
    }

    fun decodeSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.decodeSendDownstream?.addAndGet(count)
    }

    fun unpackStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.unpackStart?.addAndGet(count)
    }


    fun unpackEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.unpackEnd?.addAndGet(count)
    }

    fun unpackSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.unpackSendDownstream?.addAndGet(count)
    }

    fun filterStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterStart?.addAndGet(count)
    }

    fun filterEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterEnd?.addAndGet(count)
    }

    fun filterSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterSendDownstream?.addAndGet(count)
    }

    fun countParsePrepared(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parsePrepared?.addAndGet(count)
    }

    fun countParseRequested(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parseRequested?.addAndGet(count)
    }

    fun countParseReceivedTotal(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parseReceivedTotal?.addAndGet(count)
    }


    fun countParseReceivedFailed(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parseReceivedFailed?.addAndGet(count)
    }

    fun countFetchedMessages(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetched?.addAndGet(count)
    }

    fun countFetchedBytes(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedBytes?.addAndGet(count)
    }

    fun countFetchedBatches(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedBatches?.addAndGet(count)
    }

    fun countFilterAccepted(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterAccepted?.addAndGet(count)
    }

    fun countFilterDiscarded(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterDiscarded?.addAndGet(count)
    }

    fun countFilteredTotal(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterTotal?.addAndGet(count)
    }

    fun countMerged() {
        if (!sendPipelineStatus) return
        this.merged.incrementAndGet()
    }

    fun countSend() {
        if (!sendPipelineStatus) return
        this.sended.incrementAndGet()
    }

    fun getSnapshot(): PipelineStatusSnapshot {
        return PipelineStatusSnapshot(
            processingStartTimestamp,
            System.currentTimeMillis() - processingStartTimestamp,
            merged.get(),
            sended.get(),
            streams
        )
    }
}
