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
import com.exactpro.th2.rptdataprovider.Metrics
import com.fasterxml.jackson.annotation.JsonIgnore
import io.prometheus.client.Counter
import java.util.concurrent.atomic.AtomicLong

data class PipelineStatusSnapshot(
    val startTime: Long,
    val processingTime: Long,
    val merged: Long,
    val returned: Long,
    val counters: Map<String, PipelineStreamCounters>
)

data class PipelineStreamCounters(
    @JsonIgnore
    val fetched: AtomicLong = AtomicLong(0),
    val fetchedBytes: AtomicLong = AtomicLong(0),
    val fetchedBatches: AtomicLong = AtomicLong(0),
    val skipped: AtomicLong = AtomicLong(0),
    val skippedBatches: AtomicLong = AtomicLong(0),
    val parsePrepared: AtomicLong = AtomicLong(0),
    val parseRequested: AtomicLong = AtomicLong(0),
    val parseReceivedTotal: AtomicLong = AtomicLong(0),
    val parseReceivedFailed: AtomicLong = AtomicLong(0),
    val filterTotal: AtomicLong = AtomicLong(0),
    val filterDiscarded: AtomicLong = AtomicLong(0),
    val filterAccepted: AtomicLong = AtomicLong(0),

    val fetchedStart: AtomicLong = AtomicLong(0),
    val fetchedEnd: AtomicLong = AtomicLong(0),
    val fetchedSendDownstream: AtomicLong = AtomicLong(0),

    val convertStart: AtomicLong = AtomicLong(0),
    val convertEnd: AtomicLong = AtomicLong(0),
    val convertSendDownstream: AtomicLong = AtomicLong(0),

    val decodeStart: AtomicLong = AtomicLong(0),
    val decodeEnd: AtomicLong = AtomicLong(0),
    val decodeSendDownstream: AtomicLong = AtomicLong(0),

    val unpackStart: AtomicLong = AtomicLong(0),
    val unpackEnd: AtomicLong = AtomicLong(0),
    val unpackSendDownstream: AtomicLong = AtomicLong(0),

    val filterStart: AtomicLong = AtomicLong(0),
    val filterEnd: AtomicLong = AtomicLong(0),
    val filterSendDownstream: AtomicLong = AtomicLong(0)
)

class PipelineStatus(context: Context) {

    private val processingStartTimestamp: Long = System.currentTimeMillis()
    private val sendPipelineStatus = context.configuration.sendPipelineStatus.value.toBoolean()

    private val streams: MutableMap<String, PipelineStreamCounters> = mutableMapOf()

    var merged: AtomicLong = AtomicLong(0)
    var sended: AtomicLong = AtomicLong(0)

    companion object {
        private val fetched = Counter.build("fetched", "Count fetched").labelNames("stream").labelNames("stream").register()
        private val fetchedBytes = Counter.build("fetchedBytes", "Count fetchedBytes").labelNames("stream").register()
        private val fetchedBatches = Counter.build("fetchedBatches", "Count fetchedBatches").labelNames("stream").register()
        private val skipped = Counter.build("skipped", "Count skipped").labelNames("stream").register()
        private val skippedBatches = Counter.build("skippedBatches", "Count skippedBatches").labelNames("stream").register()
        private val parsePrepared = Counter.build("parsePrepared", "Count parsePrepared").labelNames("stream").register()
        private val parseRequested = Counter.build("parseRequested", "Count parseRequested").labelNames("stream").register()
        private val parseReceivedTotal = Counter.build("parseReceivedTotal", "Count parseReceivedTotal").labelNames("stream").register()
        private val parseReceivedFailed = Counter.build("parseReceivedFailed", "Count parseReceivedFailed").labelNames("stream").register()
        private val filterTotal = Counter.build("filterTotal", "Count filterTotal").labelNames("stream").register()
        private val filterDiscarded = Counter.build("filterDiscarded", "Count filterDiscarded").labelNames("stream").register()
        private val filterAccepted = Counter.build("filterAccepted", "Count filterAccepted").labelNames("stream").register()

        private val fetchedStart = Counter.build("fetchedStart", "Count fetchedStart").labelNames("stream").register()
        private val fetchedEnd = Counter.build("fetchedEnd", "Count fetchedEnd").labelNames("stream").register()
        private val fetchedSendDownstream =
            Counter.build("fetchedSendDownstream", "Count fetchedSendDownstream").labelNames("stream").register()

        private val convertStart = Counter.build("convertStart", "Count convertStart").labelNames("stream").register()
        private val convertEnd = Counter.build("convertEnd", "Count convertEnd").labelNames("stream").register()
        private val convertSendDownstream =
            Counter.build("convertSendDownstream", "Count convertSendDownstream").labelNames("stream").register()

        private val decodeStart = Counter.build("decodeStart", "Count decodeStart").labelNames("stream").register()
        private val decodeEnd = Counter.build("decodeEnd", "Count decodeEnd").labelNames("stream").register()
        private val decodeSendDownstream =
            Counter.build("decodeSendDownstream", "Count decodeSendDownstream").labelNames("stream").register()

        private val unpackStart = Counter.build("unpackStart", "Count unpackStart").labelNames("stream").register()
        private val unpackEnd = Counter.build("unpackEnd", "Count unpackEnd").labelNames("stream").register()
        private val unpackSendDownstream =
            Counter.build("unpackSendDownstream", "Count unpackSendDownstream").labelNames("stream").register()
        private val filterStart = Counter.build("filterStart", "Count filterStart").labelNames("stream").register()
        private val filterEnd = Counter.build("filterEnd", "Count filterEnd").labelNames("stream").register()
        private val filterSendDownstream =
            Counter.build("filterSendDownstream", "Count filterSendDownstream").labelNames("stream").register()

        private val mergedMetric = Counter.build("merged", "Count merged").labelNames("time").register()
        private val sentMetric = Counter.build("sent", "Count sent").labelNames("time").register()

        val codecLatency: Metrics =
            Metrics("th2_codec_latency", "Codec requests latency", listOf("stream"))

        private val metrics = listOf(
            fetched, fetchedBytes, fetchedBatches, parsePrepared,
            parseRequested, parseReceivedTotal, parseReceivedFailed, filterTotal,
            filterDiscarded, filterAccepted, fetchedStart, fetchedEnd,
            fetchedSendDownstream, convertStart, convertEnd, convertSendDownstream,
            decodeStart, decodeEnd, decodeSendDownstream, unpackStart, unpackEnd,
            unpackSendDownstream, filterStart, filterEnd, filterSendDownstream
        )
    }

    fun addStreams(streams: List<String>) {
        if (sendPipelineStatus) {
            for (streamName in streams) {
                this.streams[streamName] = PipelineStreamCounters()
                metrics.forEach {
                    it.labels(streamName)
                }
                codecLatency.labels(streamName)
            }
            mergedMetric.labels(processingStartTimestamp.toString())
            sentMetric.labels(processingStartTimestamp.toString())
        }
    }

    fun fetchedStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetchedStart.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetchedStart?.addAndGet(count)
    }

    fun fetchedEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetchedEnd.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetchedEnd?.addAndGet(count)
    }

    fun fetchedSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetchedSendDownstream.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetchedSendDownstream?.addAndGet(count)
    }

    fun convertStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        convertStart.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.convertStart?.addAndGet(count)
    }

    fun convertEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        convertEnd.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.convertEnd?.addAndGet(count)
    }

    fun convertSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        convertSendDownstream.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.convertSendDownstream?.addAndGet(count)
    }

    fun decodeStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        decodeStart.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.decodeStart?.addAndGet(count)
    }


    fun decodeEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        decodeEnd.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.decodeEnd?.addAndGet(count)
    }

    fun decodeSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        decodeSendDownstream.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.decodeSendDownstream?.addAndGet(count)
    }

    fun unpackStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        unpackStart.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.unpackStart?.addAndGet(count)
    }


    fun unpackEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        unpackEnd.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.unpackEnd?.addAndGet(count)
    }

    fun unpackSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        unpackSendDownstream.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.unpackSendDownstream?.addAndGet(count)
    }

    fun filterStart(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterStart.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterStart?.addAndGet(count)
    }

    fun filterEnd(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterEnd.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterEnd?.addAndGet(count)
    }

    fun filterSendDownstream(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterSendDownstream.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterSendDownstream?.addAndGet(count)
    }

    fun countParsePrepared(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        parsePrepared.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.parsePrepared?.addAndGet(count)
    }

    fun countParseRequested(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        parseRequested.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.parseRequested?.addAndGet(count)
    }

    fun countParseReceivedTotal(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        parseReceivedTotal.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.parseReceivedTotal?.addAndGet(count)
    }


    fun countParseReceivedFailed(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        parseReceivedFailed.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.parseReceivedFailed?.addAndGet(count)
    }

    fun countFetchedMessages(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetched.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetched?.addAndGet(count)
    }

    fun countFetchedBytes(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetchedBytes.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetchedBytes?.addAndGet(count)
    }

    fun countFetchedBatches(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        fetchedBatches.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.fetchedBatches?.addAndGet(count)
    }

    fun countSkippedMessages(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        skipped.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.skipped?.addAndGet(count)
    }

    fun countSkippedBatches(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        skippedBatches.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.skippedBatches?.addAndGet(count)
    }

    fun countFilterAccepted(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterAccepted.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterAccepted?.addAndGet(count)
    }

    fun countFilterDiscarded(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterDiscarded.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterDiscarded?.addAndGet(count)
    }

    fun countFilteredTotal(streamName: String, count: Long = 1) {
        if (!sendPipelineStatus) return
        filterTotal.labels(streamName).inc(count.toDouble())
        this.streams[streamName]?.filterTotal?.addAndGet(count)
    }

    fun countMerged() {
        if (!sendPipelineStatus) return
        mergedMetric.labels(processingStartTimestamp.toString()).inc()
        this.merged.incrementAndGet()
    }

    fun countSend() {
        if (!sendPipelineStatus) return
        sentMetric.labels(processingStartTimestamp.toString()).inc()
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
