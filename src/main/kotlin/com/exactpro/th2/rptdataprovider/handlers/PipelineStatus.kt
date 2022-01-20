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
import java.util.*
import java.util.concurrent.atomic.AtomicLong

data class Counters(
    val fetched: AtomicLong = AtomicLong(0),
    val fetchedBytes: AtomicLong = AtomicLong(0),
    val fetchedBatches: AtomicLong = AtomicLong(0),
    val parseRequested: AtomicLong = AtomicLong(0),
    val parseReceived: AtomicLong = AtomicLong(0),
    val filterTotal: AtomicLong = AtomicLong(0),
    val filterDiscarded: AtomicLong = AtomicLong(0),
    val filterAccepted: AtomicLong = AtomicLong(0)
)

data class StreamCounters(
    val startTime: Long,
    var timeSinceStartProcessing: Long,
    val counters: Counters
)

data class PipelineStatus(
    val streams: MutableMap<String, StreamCounters> = mutableMapOf(),
    var merger: AtomicLong = AtomicLong(0),
    private val context: Context
) {

    private val sendPipelineStatus = context.configuration.sendPipelineStatus.value.toBoolean()

    fun addStream(streamName: String) {
        if (sendPipelineStatus) {
            this.streams[streamName] = StreamCounters(
                startTime = Date().time,
                timeSinceStartProcessing = getMillisecondsDifference(Date().time),
                counters = Counters(
                    fetched = AtomicLong(0),
                    fetchedBatches = AtomicLong(0),
                    fetchedBytes = AtomicLong(0),
                    parseReceived = AtomicLong(0),
                    parseRequested = AtomicLong(0),
                    filterTotal = AtomicLong(0),
                    filterDiscarded = AtomicLong(0),
                    filterAccepted = AtomicLong(0)
                )
            )
        }

    }

    fun countParseRequested(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.parseRequested?.incrementAndGet()
            } else {
                val alreadyParsedRequested: Long = this.streams[streamName]?.counters?.parseRequested?.get()!!
                this.streams[streamName]?.counters?.parseRequested?.set(alreadyParsedRequested + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countParseReceived(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.parseReceived?.incrementAndGet()
            } else {
                val alreadyParseReceived: Long = this.streams[streamName]?.counters?.parseReceived?.get()!!
                this.streams[streamName]?.counters?.parseReceived?.set(alreadyParseReceived + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFetched(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.fetched?.incrementAndGet()
            } else {
                val alreadyFetched: Long = this.streams[streamName]?.counters?.fetched?.get()!!
                this.streams[streamName]?.counters?.fetched?.set(alreadyFetched + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFetchedBytes(streamName: String, messageBatchSize: Long = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.fetchedBytes?.incrementAndGet()
            } else {
                val alreadyFetched: Long = this.streams[streamName]?.counters?.fetchedBytes?.get()!!
                this.streams[streamName]?.counters?.fetchedBytes?.set(alreadyFetched + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFetchedBatches(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.fetchedBatches?.incrementAndGet()
            } else {
                val alreadyFetched: Long = this.streams[streamName]?.counters?.fetchedBatches?.get()!!
                this.streams[streamName]?.counters?.fetchedBatches?.set(alreadyFetched + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFilterAccepted(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.filterAccepted?.incrementAndGet()
            } else {
                val alreadyFilterAccepted: Long = this.streams[streamName]?.counters?.filterAccepted?.get()!!
                this.streams[streamName]?.counters?.filterAccepted?.set(alreadyFilterAccepted + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFilterDiscarded(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.filterDiscarded?.incrementAndGet()
            } else {
                val alreadyFilterDiscarded: Long = this.streams[streamName]?.counters?.filterDiscarded?.get()!!
                this.streams[streamName]?.counters?.filterDiscarded?.set(alreadyFilterDiscarded + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countFilteredTotal(streamName: String, messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.streams[streamName]?.counters?.filterTotal?.incrementAndGet()
            } else {
                val alreadyFiltered: Long = this.streams[streamName]?.counters?.filterTotal?.get()!!
                this.streams[streamName]?.counters?.filterTotal?.set(alreadyFiltered + messageBatchSize.toLong())
            }
        }
        setTimeValues(streamName)
    }

    fun countMerger(messageBatchSize: Int = -1) {
        if (sendPipelineStatus) {
            if (messageBatchSize < 0) {
                this.merger.incrementAndGet()
            } else {
                val alreadyMerged: Long = this.merger.get()
                this.merger.set(alreadyMerged + messageBatchSize.toLong())
            }
        }
    }

    private fun setTimeValues(streamName: String)
    {
        val recentTimeSinceStartProcessing = this.streams[streamName]?.timeSinceStartProcessing
        val startTime = this.streams[streamName]?.startTime
        this.streams[streamName]?.timeSinceStartProcessing =
            getMillisecondsDifference(startTime!!)
        val diff = this.streams[streamName]?.timeSinceStartProcessing!!/1000 - recentTimeSinceStartProcessing!!/1000

        if(diff == (1).toLong()){
            println(this)
        }
    }

    private fun getMillisecondsDifference(milliseconds: Long) : Long{
        return Date().time - milliseconds;
    }
}