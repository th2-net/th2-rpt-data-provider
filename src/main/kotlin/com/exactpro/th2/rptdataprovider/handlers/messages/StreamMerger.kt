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
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.messages.helpers.MultipleStreamHolder
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import mu.KotlinLogging
import java.time.Instant
import kotlin.coroutines.coroutineContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

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

    private val messageStreams = MultipleStreamHolder(pipelineStreams)

    private var resultCountLimit = searchRequest.resultCountLimit

    private val processJob: Job

    init {
        processJob = externalScope.launch {
            processMessage()
        }
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

    private fun inTimeRange(pipelineStepObject: PipelineStepObject): Boolean {
        return if (pipelineStepObject !is EmptyPipelineObject) {
            timestampInRange(pipelineStepObject)
        } else {
            true
        }
    }

    private suspend fun keepAliveGenerator() {
        while (coroutineContext.isActive) {
            val scannedObjectCount = messageStreams.getScannedObjectCount()
            val lastScannedObject = messageStreams.getLastScannedObject(searchRequest.searchDirection)

            val keepAlive = if (lastScannedObject != null) {
                PipelineKeepAlive(lastScannedObject, scannedObjectCount)
            } else {
                PipelineKeepAlive(false, null, Instant.ofEpochMilli(0), scannedObjectCount)
            }
            logger.trace { "Emitting keep alive object $keepAlive" }
            sendToChannel(keepAlive)
            delay(context.keepAliveTimeout)
        }
        logger.debug { "Keep alive generation canceled" }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        coroutineScope {

            val job = launch {
                try {
                    keepAliveGenerator()
                } catch (ex: Exception) {
                    logger.debug(ex) { "keep alive exception handled" }
                }
            }

            messageStreams.init()

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
            } while (!messageStreams.isAllStreamEmpty() && (resultCountLimit?.let { it > 0 } != false) && inTimeRange)

            sendToChannel(StreamEndObject(false, null, Instant.ofEpochMilli(0)))
            logger.debug { "StreamEndObject has been sent" }
            job.cancelAndJoin()
            logger.debug { "Merging is finished" }
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
                    messageStreams.getLoggingStreamInfo()
                else null

            let {
                if (searchRequest.searchDirection == TimeRelation.AFTER) {
                    messageStreams.selectMessage { new, old ->
                        isLess(new, old)
                    }
                } else {
                    messageStreams.selectMessage { new, old ->
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

    suspend fun getStreamsInfo(): List<StreamInfo> {
        logger.info { "Getting streams info" }
        processJob.join()
        logger.debug { "Merge job is finished" }
        return messageStreams.getStreamsInfo { current, prev ->
            if (searchRequest.searchDirection == TimeRelation.AFTER) {
                current > prev
            } else {
                current < prev
            }
        }
    }
}
