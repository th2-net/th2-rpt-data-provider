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
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import io.ktor.util.pipeline.*
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.time.Instant


private class StreamHolder(val messageStream: PipelineComponent) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    var currentElement: PipelineStepObject? = null
        private set
    var previousElement: PipelineStepObject? = null
        private set


    private fun changePreviousElement(currentElement: PipelineStepObject?) {
        if (previousElement == null
            || previousElement is EmptyPipelineObject
            || currentElement is PipelineFilteredMessage
        ) {
            previousElement = currentElement
        }
    }

    fun top(): PipelineStepObject {
        return currentElement!!
    }

    suspend fun pop(): PipelineStepObject {
        return messageStream.pollMessage().let {
            logger.trace { it.lastProcessedId }
            if (previousElement == null && currentElement == null) {
                logger.trace { it.lastProcessedId }
                previousElement = it
                currentElement = it
            } else {
                logger.trace { it.lastProcessedId }
                changePreviousElement(currentElement)
                currentElement = it
            }
            previousElement!!
        }
    }
}


class StreamMerger(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    externalScope: CoroutineScope,
    pipelineStreams: List<PipelineComponent>,
    messageFlowCapacity: Int
) : PipelineComponent(context, searchRequest, externalScope, messageFlowCapacity = messageFlowCapacity) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }


    private val messageStreams = pipelineStreams.map { StreamHolder(it) }
    private var allStreamIsEmpty: Boolean = false
    private var resultCountLimit = searchRequest.resultCountLimit

    init {
        externalScope.launch {
            processMessage()
        }
    }

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

    private fun keepSearch(): Boolean {
        val isKeepOpen = searchRequest.keepOpen && searchRequest.searchDirection == TimeRelation.AFTER
        val haveNotReachedLimit = resultCountLimit?.let { it > 0 } ?: true
        return (!allStreamIsEmpty || isKeepOpen) && haveNotReachedLimit
    }

    private fun inTimeRange(pipelineStepObject: PipelineStepObject): Boolean {
        return if (pipelineStepObject !is EmptyPipelineObject) {
            timestampInRange(pipelineStepObject)
        } else {
            true
        }
    }

    private fun getLastScannedObject(): PipelineStepObject? {
        return if (searchRequest.searchDirection == TimeRelation.AFTER) {
            messageStreams
                .minBy { it.previousElement?.lastScannedTime ?: Instant.MAX }
                ?.previousElement
        } else {
            messageStreams
                .maxBy { it.previousElement?.lastScannedTime ?: Instant.MIN }
                ?.previousElement
        }
    }

    private fun getScannedObjectCount(): Long {
        return messageStreams
            .map { it.messageStream.processedMessageCount }
            .reduceRight { acc, value -> acc + value }
    }

    private suspend fun keepAliveGenerator(coroutineScope: CoroutineScope) {
        while (coroutineScope.isActive) {
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

            launch { keepAliveGenerator(this@coroutineScope) }

            messageStreamsInit()
            do {

                val nextMessage = getNextMessage()

                val inTimeRange = inTimeRange(nextMessage)

                logger.trace { nextMessage.lastProcessedId }

                if (nextMessage !is EmptyPipelineObject && inTimeRange) {
                    logger.trace { nextMessage.lastProcessedId }
                    sendToChannel(nextMessage)
                    resultCountLimit = resultCountLimit?.dec()
                }

            } while (keepSearch() && inTimeRange)

            sendToChannel(StreamEndObject(false, null, Instant.ofEpochMilli(0)))
        }
    }

    private fun isStreamEmpty(streamsEmpty: Boolean, messageStream: StreamHolder): Boolean {
        return streamsEmpty && messageStream.top().streamEmpty && messageStream.top() is EmptyPipelineObject
    }

    private suspend fun selectMessage(comparator: (PipelineStepObject, PipelineStepObject) -> Boolean): PipelineStepObject {
        return coroutineScope {
            var resultElement: StreamHolder = messageStreams.first()
            var streamsEmpty = true
            for (messageStream in messageStreams) {
                streamsEmpty = isStreamEmpty(streamsEmpty, messageStream)
                if (comparator(messageStream.top(), resultElement.top())) {
                    resultElement = messageStream
                }
            }
            allStreamIsEmpty = streamsEmpty
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