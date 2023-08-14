/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.th2.rptdataprovider.Metrics
import io.prometheus.client.Counter

class PipelineStatus {

    private val processingStartTimestamp: Long = System.currentTimeMillis()

    companion object {
        private val fetched =
            Counter.build("fetched", "Count fetched").labelNames("stream").labelNames("stream").register()
        private val fetchedBytes = Counter.build("fetchedBytes", "Count fetchedBytes").labelNames("stream").register()
        private val fetchedBatches =
            Counter.build("fetchedBatches", "Count fetchedBatches").labelNames("stream").register()
        private val skipped = Counter.build("skipped", "Count skipped").labelNames("stream").register()
        private val skippedBatches =
            Counter.build("skippedBatches", "Count skippedBatches").labelNames("stream").register()
        private val parsePrepared =
            Counter.build("parsePrepared", "Count parsePrepared").labelNames("stream").register()
        private val parseRequested =
            Counter.build("parseRequested", "Count parseRequested").labelNames("stream").register()
        private val parseReceivedTotal =
            Counter.build("parseReceivedTotal", "Count parseReceivedTotal").labelNames("stream").register()
        private val parseReceivedFailed =
            Counter.build("parseReceivedFailed", "Count parseReceivedFailed").labelNames("stream").register()
        private val filterTotal = Counter.build("filterTotal", "Count filterTotal").labelNames("stream").register()
        private val filterDiscarded =
            Counter.build("filterDiscarded", "Count filterDiscarded").labelNames("stream").register()
        private val filterAccepted =
            Counter.build("filterAccepted", "Count filterAccepted").labelNames("stream").register()

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
        for (streamName in streams) {
            metrics.forEach {
                it.labels(streamName)
            }
            codecLatency.labels(streamName)
        }
        mergedMetric.labels(processingStartTimestamp.toString())
        sentMetric.labels(processingStartTimestamp.toString())
    }

    fun fetchedStart(streamName: String, count: Long = 1) {
        fetchedStart.labels(streamName).inc(count.toDouble())
    }

    fun fetchedEnd(streamName: String, count: Long = 1) {
        fetchedEnd.labels(streamName).inc(count.toDouble())
    }

    fun fetchedSendDownstream(streamName: String, count: Long = 1) {
        fetchedSendDownstream.labels(streamName).inc(count.toDouble())
    }

    fun convertStart(streamName: String, count: Long = 1) {
        convertStart.labels(streamName).inc(count.toDouble())
    }

    fun convertEnd(streamName: String, count: Long = 1) {
        convertEnd.labels(streamName).inc(count.toDouble())
    }

    fun convertSendDownstream(streamName: String, count: Long = 1) {
        convertSendDownstream.labels(streamName).inc(count.toDouble())
    }

    fun decodeStart(streamName: String, count: Long = 1) {
        decodeStart.labels(streamName).inc(count.toDouble())
    }


    fun decodeEnd(streamName: String, count: Long = 1) {
        decodeEnd.labels(streamName).inc(count.toDouble())
    }

    fun decodeSendDownstream(streamName: String, count: Long = 1) {
        decodeSendDownstream.labels(streamName).inc(count.toDouble())
    }

    fun unpackStart(streamName: String, count: Long = 1) {
        unpackStart.labels(streamName).inc(count.toDouble())
    }


    fun unpackEnd(streamName: String, count: Long = 1) {
        unpackEnd.labels(streamName).inc(count.toDouble())
    }

    fun unpackSendDownstream(streamName: String, count: Long = 1) {
        unpackSendDownstream.labels(streamName).inc(count.toDouble())
    }

    fun filterStart(streamName: String, count: Long = 1) {
        filterStart.labels(streamName).inc(count.toDouble())
    }

    fun filterEnd(streamName: String, count: Long = 1) {
        filterEnd.labels(streamName).inc(count.toDouble())
    }

    fun filterSendDownstream(streamName: String, count: Long = 1) {
        filterSendDownstream.labels(streamName).inc(count.toDouble())
    }

    fun countParsePrepared(streamName: String, count: Long = 1) {
        parsePrepared.labels(streamName).inc(count.toDouble())
    }

    fun countParseRequested(streamName: String, count: Long = 1) {
        parseRequested.labels(streamName).inc(count.toDouble())
    }

    fun countParseReceivedTotal(streamName: String, count: Long = 1) {
        parseReceivedTotal.labels(streamName).inc(count.toDouble())
    }


    fun countParseReceivedFailed(streamName: String, count: Long = 1) {
        parseReceivedFailed.labels(streamName).inc(count.toDouble())
    }

    fun countFetchedMessages(streamName: String, count: Long = 1) {
        fetched.labels(streamName).inc(count.toDouble())
    }

    fun countFetchedBytes(streamName: String, count: Long = 1) {
        fetchedBytes.labels(streamName).inc(count.toDouble())
    }

    fun countFetchedBatches(streamName: String, count: Long = 1) {
        fetchedBatches.labels(streamName).inc(count.toDouble())
    }

    fun countSkippedMessages(streamName: String, count: Long = 1) {
        skipped.labels(streamName).inc(count.toDouble())
    }

    fun countSkippedBatches(streamName: String, count: Long = 1) {
        skippedBatches.labels(streamName).inc(count.toDouble())
    }

    fun countFilterAccepted(streamName: String, count: Long = 1) {
        filterAccepted.labels(streamName).inc(count.toDouble())
    }

    fun countFilterDiscarded(streamName: String, count: Long = 1) {
        filterDiscarded.labels(streamName).inc(count.toDouble())
    }

    fun countFilteredTotal(streamName: String, count: Long = 1) {
        filterTotal.labels(streamName).inc(count.toDouble())
    }

    fun countMerged() {
        mergedMetric.labels(processingStartTimestamp.toString()).inc()
    }

    fun countSend() {
        sentMetric.labels(processingStartTimestamp.toString()).inc()
    }
}
