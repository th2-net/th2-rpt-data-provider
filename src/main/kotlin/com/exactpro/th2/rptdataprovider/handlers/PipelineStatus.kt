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
import java.util.concurrent.atomic.AtomicLong

data class PipelineStatusSnapshot(
    val startTime: Long,
    val processingTime: Long,
    val returned: Long,
    val counters: Map<String, PipelineStreamCounters>
)

data class PipelineStreamCounters(
    val fetched: AtomicLong = AtomicLong(0),
    val fetchedBytes: AtomicLong = AtomicLong(0),
    val fetchedBatches: AtomicLong = AtomicLong(0),
    val parsePrepared: AtomicLong = AtomicLong(0),
    val parseRequested: AtomicLong = AtomicLong(0),
    val parseReceivedTotal: AtomicLong = AtomicLong(0),
    val parseReceivedFailed: AtomicLong = AtomicLong(0),
    val filterTotal: AtomicLong = AtomicLong(0),
    val filterDiscarded: AtomicLong = AtomicLong(0),
    val filterAccepted: AtomicLong = AtomicLong(0)
)

class PipelineStatus(context: Context) {

    val streams: MutableMap<String, PipelineStreamCounters> = mutableMapOf()
    var merged: AtomicLong = AtomicLong(0)

    private val processingStartTimestamp: Long = System.currentTimeMillis()
    private val sendPipelineStatus = context.configuration.sendPipelineStatus.value.toBoolean()

    fun addStream(streamName: String) {
        if (sendPipelineStatus) {
            this.streams[streamName] = PipelineStreamCounters()
        }
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

    fun getSnapshot(): PipelineStatusSnapshot {
        return PipelineStatusSnapshot(
            processingStartTimestamp,
            System.currentTimeMillis() - processingStartTimestamp,
            merged.get(),
            streams
        )
    }
}