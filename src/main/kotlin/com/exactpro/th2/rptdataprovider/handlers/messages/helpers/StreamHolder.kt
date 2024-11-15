/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers.messages.helpers

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidInitializationException
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import io.prometheus.client.Histogram
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class StreamHolder(val messageStream: PipelineComponent<*, *, *, *>) {

    companion object {
        private val pullFromStream = Histogram.build(
            "th2_stream_pull_time", "Time of stream pull"
        ).buckets(.0001, .0005, .001, .005, .01)
            .labelNames("stream")
            .register()
    }

    private val streamName = messageStream.commonStreamName?.toString()
    private val labelMetric = pullFromStream.labels(streamName)

    var currentElement: PipelineStepObject? = null
        private set

    var previousElement: PipelineStepObject? = null
        private set

    fun top(): PipelineStepObject {
        return currentElement!!
    }

    suspend fun init() {
        messageStream.pollMessage().let {
            if (previousElement == null && currentElement == null) {
                currentElement = it
            } else {
                throw InvalidInitializationException("StreamHolder ${messageStream.commonStreamName} already initialized")
            }
        }
    }

    @OptIn(ExperimentalTime::class)
    suspend fun pop(): PipelineStepObject {
        return measureTimedValue {
            messageStream.pollMessage().let { newElement ->
                val currentElementTemporary = currentElement

                currentElementTemporary?.also {
                    previousElement = currentElement
                    currentElement = newElement
                }
                    ?: throw InvalidInitializationException("StreamHolder ${messageStream.commonStreamName} need initialization")
            }
        }.let {
            labelMetric.observe(it.duration.toDouble(DurationUnit.SECONDS))
            it.value
        }
    }

    private fun isNeedSearchResumeId(sequenceFilter: (current: Long, prev: Long) -> Boolean): Boolean {
        return currentElement?.let {
            !it.streamEmpty && !sequenceFilter(it.lastProcessedId?.sequence ?: 0, previousElement?.lastProcessedId?.sequence ?: 0)
        } ?: false
    }

    suspend fun getStreamInfo(sequenceFilter: (current: Long, prev: Long) -> Boolean): StreamInfo {
        while (isNeedSearchResumeId(sequenceFilter)) {
            pop()
        }
        val streamName = checkNotNull(messageStream.commonStreamName) { "stream name is null" }
        val stepObject = currentElement
        return if (stepObject != null && stepObject.streamEmpty) {
            StreamInfo(streamName, null)
        } else {
            StreamInfo(streamName, stepObject?.lastProcessedId)
        }
    }
}