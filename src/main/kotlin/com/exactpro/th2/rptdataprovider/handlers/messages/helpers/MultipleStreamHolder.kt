/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant

class MultipleStreamHolder(pipelineComponents: List<PipelineComponent<*, *, *, *>>) {

    private val messageStreams = pipelineComponents.map { StreamHolder(it) }

    private val mutex = Mutex()

    private fun isStreamEmpty(streamsEmpty: Boolean, messageStream: StreamHolder): Boolean {
        return streamsEmpty && messageStream.top().streamEmpty && messageStream.top() is EmptyPipelineObject
    }

    fun getLastScannedObject(direction: TimeRelation): PipelineStepObject? {
        return if (direction == TimeRelation.AFTER) {
            messageStreams
                .maxByOrNull { it.currentElement?.lastScannedTime ?: Instant.MIN }
                ?.previousElement
        } else {
            messageStreams
                .minByOrNull { it.currentElement?.lastScannedTime ?: Instant.MIN }
                ?.previousElement
        }
    }

    fun getScannedObjectCount(): Long {
        return messageStreams
            .map { it.messageStream.processedMessageCount }
            .reduceRight { acc, value -> acc + value }
    }

    fun getLoggingStreamInfo(): String {
        return messageStreams.joinToString(", ") {
            "${it.top().lastProcessedId} - ${it.top().lastScannedTime}"
        }
    }

    suspend fun init() {
        messageStreams.forEach { it.init() }
    }

    /**
     * @param sequenceFilter filters the sequence from pipeline object to get the correct resume ID
     */
    suspend fun getStreamsInfo(sequenceFilter: (current: Long, prev: Long) -> Boolean): List<StreamInfo> {
        return mutex.withLock {
            messageStreams.map { it.getStreamInfo(sequenceFilter) }
        }
    }

    suspend fun selectMessage(comparator: (PipelineStepObject, PipelineStepObject) -> Boolean): PipelineStepObject {
        return mutex.withLock {
            var resultElement: StreamHolder = messageStreams.first()
            for (messageStream in messageStreams) {
                if (comparator(messageStream.top(), resultElement.top())) {
                    resultElement = messageStream
                }
            }
            resultElement.pop()
        }
    }

    suspend fun isAllStreamEmpty(): Boolean {
        return mutex.withLock {
            var streamsEmpty = true
            for (stream in messageStreams) {
                streamsEmpty = isStreamEmpty(streamsEmpty, stream)
            }
            streamsEmpty
        }
    }
}