/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineKeepAlive
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.internal.StreamEndObject
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.messages.helpers.MultipleStreamHolder
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import kotlin.coroutines.coroutineContext
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

//FIXME: Check stream stop condition and streaminfo object
class StreamMerger<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    searchRequest: SseMessageSearchRequest<RM, PM>,
    externalScope: CoroutineScope,
    pipelineStreams: List<PipelineComponent<B, G, RM, PM>>,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent<B, G, RM, PM>(context, searchRequest, externalScope, messageFlowCapacity = messageFlowCapacity) {
    private val messageStreams = MultipleStreamHolder(pipelineStreams)
    private var resultCountLimit = searchRequest.resultCountLimit

    private val messageFilterByTimestamp: (PipelineStepObject, PipelineStepObject) -> Boolean = when(searchRequest.searchDirection) {
        TimeRelation.AFTER -> { msg1, msg2 -> msg1.lastScannedTime.isBefore(msg2.lastScannedTime) }
        TimeRelation.BEFORE -> { msg1, msg2 -> msg2.lastScannedTime.isBefore(msg1.lastScannedTime) }
    }

    private val timestampFilter = when (searchRequest.searchDirection) {
        TimeRelation.AFTER -> Instant::isBeforeOrEqual
        TimeRelation.BEFORE -> Instant::isAfterOrEqual
    }

    private val sequenceFilter: (Long, Long) -> Boolean = when(searchRequest.searchDirection) {
        TimeRelation.AFTER -> { current, prev -> current > prev }
        TimeRelation.BEFORE -> { current, prev -> current < prev }
    }

    private val processJob: Job
    init {
        processJob = externalScope.launch { processMessage() }
    }

    private fun inTimeRange(pipelineStepObject: PipelineStepObject): Boolean =
        pipelineStepObject is EmptyPipelineObject
                || searchRequest.endTimestamp == null
                || timestampFilter(pipelineStepObject.lastScannedTime, searchRequest.endTimestamp)

    private suspend fun keepAliveGenerator() {
        while (coroutineContext.isActive) {
            val scannedObjectCount = messageStreams.getScannedObjectCount()
            val lastScannedObject = messageStreams.getLastScannedObject(searchRequest.searchDirection)

            val keepAlive = if (lastScannedObject != null) {
                PipelineKeepAlive(lastScannedObject, scannedObjectCount)
            } else {
                PipelineKeepAlive(false, null, Instant.ofEpochMilli(0), scannedObjectCount)
            }
            LOGGER.trace { "Emitting keep alive object $keepAlive" }
            sendToChannel(keepAlive)
            delay(context.keepAliveTimeout)
        }
        LOGGER.debug { "Keep alive generation canceled" }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        coroutineScope {

            val job = launch {
                try {
                    keepAliveGenerator()
                } catch (ex: Exception) {
                    LOGGER.debug(ex) { "keep alive exception handled" }
                }
            }

            messageStreams.init()

            do {
                val nextMessage = measureTimedValue {
                    getNextMessage()
                }.let {
                    StreamWriter.setMerging(it.duration.toDouble(DurationUnit.MILLISECONDS).toLong())
                    it.value
                }

                val inTimeRange = inTimeRange(nextMessage)

                if (nextMessage !is EmptyPipelineObject && inTimeRange) {
                    sendToChannel(nextMessage)
                    resultCountLimit = resultCountLimit?.dec()
                    pipelineStatus.countMerged()

                    LOGGER.trace {
                        nextMessage.let {
                            "message ${it.lastProcessedId} (streamEmpty=${it.streamEmpty}) with timestamp ${it.lastScannedTime} has been sent downstream"
                        }
                    }
                } else {
                    LOGGER.trace {
                        nextMessage.let {
                            "skipped message ${it.lastProcessedId} (streamEmpty=${it.streamEmpty}) with timestamp ${it.lastScannedTime}"
                        }
                    }
                }
            } while (!messageStreams.isAllStreamEmpty() && (resultCountLimit?.let { it > 0 } != false) && inTimeRange)

            sendToChannel(StreamEndObject(false, null, Instant.ofEpochMilli(0)))
            LOGGER.debug { "StreamEndObject has been sent" }
            job.cancelAndJoin()
            LOGGER.debug { "Merging is finished" }
        }
    }

    private suspend fun getNextMessage(): PipelineStepObject = coroutineScope {
        val streams = if (LOGGER.isTraceEnabled()) messageStreams.getLoggingStreamInfo() else null
        messageStreams.selectMessage(messageFilterByTimestamp).also {
            LOGGER.trace {
                "selected ${it.lastProcessedId} - ${it.javaClass.kotlin}-${it.javaClass.hashCode()} ${it.lastScannedTime} out of [${streams}]"
            }
        }
    }

    suspend fun getStreamsInfo(): List<StreamInfo> {
        LOGGER.info { "Getting streams info" }
        processJob.join()
        LOGGER.debug { "Merge job is finished" }
        return messageStreams.getStreamsInfo(sequenceFilter)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}