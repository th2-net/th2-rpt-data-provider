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

data class PipelineStatusSnapshot (
    val startTime: Long,
    val processingTime: Long,
    val returned: Long,
    val counters: Map<String, PipelineStreamCounters>
)

data class PipelineStreamCounters(
    val fetched: AtomicLong = AtomicLong(0),
    val fetchedBytes: AtomicLong = AtomicLong(0),
    val fetchedBatches: AtomicLong = AtomicLong(0),
    val parseRequested: AtomicLong = AtomicLong(0),
    val parseReceived: AtomicLong = AtomicLong(0),
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
            this.streams[streamName] = PipelineStreamCounters(
                fetched = AtomicLong(0),
                fetchedBatches = AtomicLong(0),
                fetchedBytes = AtomicLong(0),
                parseReceived = AtomicLong(0),
                parseRequested = AtomicLong(0),
                filterTotal = AtomicLong(0),
                filterDiscarded = AtomicLong(0),
                filterAccepted = AtomicLong(0)
            )
        }
    }

    fun countParseRequested(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parseRequested?.incrementAndGet()
    }

    fun countParseReceived(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.parseReceived?.incrementAndGet()
    }

    fun countFetchedMessages(streamName: String, count: Long) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetched?.addAndGet(count)
    }

    fun countFetchedBytes(streamName: String, count: Long) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedBytes?.addAndGet(count)
    }

    fun countFetchedBatches(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.fetchedBatches?.incrementAndGet()
    }

    fun countFilterAccepted(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterAccepted?.incrementAndGet()
    }

    fun countFilterDiscarded(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterDiscarded?.incrementAndGet()
    }

    fun countFilteredTotal(streamName: String) {
        if (!sendPipelineStatus) return
        this.streams[streamName]?.filterTotal?.incrementAndGet()
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
