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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineKeepAlive
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.internal.StreamEndObject
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import kotlinx.coroutines.*
import java.time.Instant
import kotlin.coroutines.coroutineContext


class StreamMerger(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    externalScope: CoroutineScope,
    pipelineStreams: List<PipelineComponent>,
    messageFlowCapacity: Int
) : PipelineComponent(context, searchRequest, externalScope, messageFlowCapacity = messageFlowCapacity) {

    private class StreamHolder(
        val messageStream: PipelineComponent
    ) {

        var currentElement: PipelineStepObject? = null
            private set
        var previousElement: PipelineStepObject? = null
            private set

        fun top(): PipelineStepObject {
            return currentElement!!
        }

        suspend fun pop(): PipelineStepObject {
            return messageStream.pollMessage().also {
                previousElement = currentElement
                currentElement = it
            }
        }
    }

    private val messageStreams = pipelineStreams.map { StreamHolder(it) }
    private var allStreamIsEmpty: Boolean = false

    private suspend fun messageStreamsInit() {
        messageStreams.forEach { it.pop() }
    }


    private fun timestampInRange(pipelineStepObject: PipelineStepObject): Boolean {
        return pipelineStepObject.lastScannedTime.let { timestamp ->
            if (searchRequest.searchDirection == TimeRelation.AFTER) {
                searchRequest.endTimestamp == null || timestamp.isBeforeOrEqual(searchRequest.endTimestamp)
            } else {
                searchRequest.endTimestamp == null || timestamp.isAfterOrEqual(searchRequest.endTimestamp)
            }
        }
    }


    private fun keepSearch(pipelineStepObject: PipelineStepObject): Boolean {
        val isKeepOpen = searchRequest.keepOpen && searchRequest.searchDirection == TimeRelation.AFTER
        return (!allStreamIsEmpty || isKeepOpen) && timestampInRange(pipelineStepObject)
    }


    private fun getLastScannedObject(): PipelineStepObject? {
        return if (searchRequest.searchDirection == TimeRelation.AFTER) {
            messageStreams
                .maxBy { it.previousElement?.lastScannedTime ?: Instant.MIN }
                ?.previousElement
        } else {
            messageStreams
                .minBy { it.previousElement?.lastScannedTime ?: Instant.MAX }
                ?.previousElement
        }
    }


    private fun getScannedObjectCount(): Long {
        return messageStreams
            .map { it.messageStream.processedMessageCount }
            .reduceRight { acc, value -> acc + value }
    }


    private suspend fun keepAliveGenerator() {
        while (coroutineContext.isActive) {
            val scannedObjectCount = getScannedObjectCount()
            val lastScannedObject = getLastScannedObject()

            if (lastScannedObject != null) {
                sendToChannel(PipelineKeepAlive(lastScannedObject, scannedObjectCount))
            } else {
                sendToChannel(PipelineKeepAlive(false, null, Instant.ofEpochMilli(0), scannedObjectCount))
            }
            delay(context.keepAliveTimeout)
        }
    }


    override suspend fun processMessage() {
        coroutineScope {

            externalScope.launch { keepAliveGenerator() }

            messageStreamsInit()
            do {

                val nextMessage = getNextMessage()

                if (nextMessage !is EmptyPipelineObject) {
                    sendToChannel(nextMessage)
                }

            } while (keepSearch(nextMessage))

            sendToChannel(StreamEndObject(false, null, Instant.ofEpochMilli(0)))
        }
    }


    private suspend fun selectMessage(comparator: (PipelineStepObject, PipelineStepObject) -> Boolean): PipelineStepObject {
        return coroutineScope {
            var resultElement: StreamHolder = messageStreams.first()
            for (messageStream in messageStreams) {
                allStreamIsEmpty = messageStream.top().streamEmpty
                if (comparator(messageStream.top(), resultElement.top())) {
                    resultElement = messageStream
                }
            }
            resultElement.pop()
        }
    }


    private fun isLess(firstMessage: PipelineStepObject, secondMessage: PipelineStepObject): Boolean {
        return firstMessage.lastScannedTime.isBefore(secondMessage.lastScannedTime)
    }


    private fun isGreater(firstMessage: PipelineStepObject, secondMessage: PipelineStepObject): Boolean {
        return firstMessage.lastScannedTime.isAfter(secondMessage.lastScannedTime)
    }


    private suspend fun getNextMessage(): PipelineStepObject {
        return coroutineScope {
            if (searchRequest.searchDirection == TimeRelation.AFTER) {
                selectMessage { new, old ->
                    isLess(new, old)
                }
            } else {
                selectMessage { new, old ->
                    isGreater(new, old)
                }
            }
        }
    }


    fun getStreamsInfo(): List<StreamInfo> {
        return messageStreams.map {
            StreamInfo(
                it.messageStream.streamName!!,
                it.previousElement?.lastProcessedId
            )
        }
    }

}