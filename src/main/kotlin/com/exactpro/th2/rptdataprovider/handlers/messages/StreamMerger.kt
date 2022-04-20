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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidInitializationException
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
<<<<<<< HEAD
import io.ktor.util.pipeline.*
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.time.Instant


private class StreamHolder(val messageStream: PipelineComponent) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    val startId = messageStream.startId

    var currentElement: PipelineStepObject? = null
        private set
    var previousElement: PipelineStepObject? = null
        private set


    private fun changePreviousElement(currentElement: PipelineStepObject?) {
        if (previousElement == null || currentElement is PipelineFilteredMessage) {
            previousElement = currentElement
        }
    }

    fun top(): PipelineStepObject {
        return currentElement!!
    }

    suspend fun init() {
        messageStream.pollMessage().let {
            if (previousElement == null && currentElement == null) {
                logger.trace { it.lastProcessedId }
                currentElement = it
            } else {
                throw InvalidInitializationException("StreamHolder ${messageStream.streamName} already initialized")
            }
        }
    }

    suspend fun pop(): PipelineStepObject {
        return messageStream.pollMessage().let { newElement ->
            val currentElementTemporary = currentElement

            currentElementTemporary?.also {
                logger.trace { newElement.lastProcessedId }
                changePreviousElement(currentElement)
                currentElement = newElement
            } ?: throw InvalidInitializationException("StreamHolder ${messageStream.streamName} need initialization")
        }
    }
}

=======
import io.prometheus.client.Histogram
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.time.Instant
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue
>>>>>>> master

//FIXME: Check stream stop condition and streaminfo object
class StreamMerger(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    externalScope: CoroutineScope,
    pipelineStreams: List<PipelineComponent>,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent(context, searchRequest, externalScope, messageFlowCapacity = messageFlowCapacity) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private class StreamHolder(val messageStream: PipelineComponent) {

        companion object {
            private val logger = KotlinLogging.logger { }
            private val pullFromStream = Histogram.build(
                "th2_stream_pull_time", "Time of stream pull"
            ).buckets(.0001, .0005, .001, .005, .01)
                .labelNames("stream")
                .register()
        }

        private val streamName = messageStream.streamName.toString()
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
                    throw InvalidInitializationException("StreamHolder ${messageStream.streamName} already initialized")
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
                        ?: throw InvalidInitializationException("StreamHolder ${messageStream.streamName} need initialization")
                }
            }.let {
                labelMetric.observe(it.duration.inSeconds)
                it.value
            }
        }
    }

    private val messageStreams = pipelineStreams.map { StreamHolder(it) }
    private var allStreamIsEmpty: Boolean = false
    private var resultCountLimit = searchRequest.resultCountLimit

    init {
        externalScope.launch {
            processMessage()
        }
    }

<<<<<<< HEAD
    private suspend fun messageStreamsInit() {
        messageStreams.forEach { it.init() }
    }

=======
>>>>>>> master
    private fun timestampInRange(pipelineStepObject: PipelineStepObject): Boolean {
        return pipelineStepObject.lastScannedTime.let { timestamp ->
            if (searchRequest.searchDirection == TimeRelation.AFTER) {
                searchRequest.endTimestamp == null || (timestamp.isBeforeOrEqual(searchRequest.endTimestamp))
            } else {
                searchRequest.endTimestamp == null || (timestamp.isAfterOrEqual(searchRequest.endTimestamp))
            }
        }
    }

<<<<<<< HEAD
    private fun keepSearch(): Boolean {
        val isKeepOpen = searchRequest.keepOpen && searchRequest.searchDirection == TimeRelation.AFTER
        val haveNotReachedLimit = resultCountLimit?.let { it > 0 } ?: true
        return (!allStreamIsEmpty || isKeepOpen) && haveNotReachedLimit
    }
=======
>>>>>>> master

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
                .maxBy { it.currentElement?.lastScannedTime ?: Instant.MIN }
                ?.previousElement
        } else {
            messageStreams
                .minBy { it.currentElement?.lastScannedTime ?: Instant.MIN }
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

<<<<<<< HEAD
=======
    @OptIn(ExperimentalTime::class)
>>>>>>> master
    override suspend fun processMessage() {
        coroutineScope {

            launch { keepAliveGenerator(this@coroutineScope) }

            messageStreams.forEach { it.init() }

            do {
                val nextMessage = measureTimedValue {
                    getNextMessage()
                }.let {
                    StreamWriter.setMerging(it.duration.inMilliseconds.toLong())
                    it.value
                }

                val inTimeRange = inTimeRange(nextMessage)

                if (nextMessage !is EmptyPipelineObject && inTimeRange) {
                    sendToChannel(nextMessage)
                    resultCountLimit = resultCountLimit?.dec()
                    pipelineStatus.countMerged()

                    logger.trace {
                        nextMessage.let {
                            "message ${it.lastProcessedId} (streamEmpty=${it.streamEmpty}) with timestamp ${it.lastScannedTime} has been sent downstream"
                        }
                    }
                } else {
                    logger.trace {
                        nextMessage.let {
                            "skipped message ${it.lastProcessedId} (streamEmpty=${it.streamEmpty}) with timestamp ${it.lastScannedTime}"
                        }
                    }
                }
            } while (!allStreamIsEmpty && (resultCountLimit?.let { it > 0 } != false) && inTimeRange)

            sendToChannel(StreamEndObject(false, null, Instant.ofEpochMilli(0)))
            logger.debug { "StreamEndObject has been sent" }
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

            val streams =
                if (logger.isTraceEnabled)
                    messageStreams.joinToString(", ") {
                        "${it.top().lastProcessedId} - ${it.top().lastScannedTime}"
                    }
                else null

            let {
                if (searchRequest.searchDirection == TimeRelation.AFTER) {
                    selectMessage { new, old ->
                        isLess(new, old)
                    }
                } else {
                    selectMessage { new, old ->
                        isGreater(new, old)
                    }
                }
            }.also {
                logger.trace {
                    "selected ${it.lastProcessedId} - ${it.javaClass.kotlin}-${it.javaClass.hashCode()} ${it.lastScannedTime} out of [${streams}]"
                }
            }
        }
    }

    fun getStreamsInfo(): List<StreamInfo> {
        return messageStreams.map {
            val storedMessageId = if (it.currentElement != null && it.currentElement?.streamEmpty!!) {
                StoredMessageId(it.messageStream.streamName?.name, it.messageStream.streamName!!.direction, -1)
            } else {
                it.currentElement?.lastProcessedId
            }

            StreamInfo(
                it.messageStream.streamName!!,
                storedMessageId
            )
        }
    }
}
