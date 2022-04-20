﻿/*******************************************************************************
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

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class MessageFilter(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent?,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent(
    context,
    searchRequest,
    externalScope,
    streamName,
    previousComponent,
    messageFlowCapacity
) {

    private val sendEmptyDelay: Long = context.configuration.sendEmptyDelay.value.toLong()
    private var lastScannedObject: PipelineStepObject? = null
    private val id = COUNTER.incrementAndGet()


    init {
        externalScope.launch {
            processMessage()
        }
    }


    constructor(
        pipelineComponent: MessageBatchUnpacker,
        messageFlowCapacity: Int,
        pipelineStatus: PipelineStatus
    ) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.streamName,
        pipelineComponent.externalScope,
        pipelineComponent,
        messageFlowCapacity,
        pipelineStatus
    )

    private fun updateState(parsedMessage: PipelineParsedMessage) {
        lastScannedObject = parsedMessage
        processedMessagesCounter++
    }


    private fun applyFilter(parsedMessage: Message): MessageWithMetadata {
        return MessageWithMetadata(parsedMessage).apply {
            finalFiltered = searchRequest.filterPredicate.apply(this)
        }
    }


    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        coroutineScope {
            while (isActive) {
                val parsedMessage = previousComponent!!.pollMessage()

                if (parsedMessage is PipelineParsedMessage) {

                    pipelineStatus.filterStart(streamName.toString())
                    val timeStart = System.currentTimeMillis()
                    val filtered = measureTimedValue {
                        pipelineStatus.countFilteredTotal(streamName.toString())
                        updateState(parsedMessage)

                        applyFilter(parsedMessage.payload)
                    }.also {
                        logger.trace { "message filtering took ${it.duration.inMilliseconds}ms (stream=$streamName sequence=${parsedMessage.payload.id.index})" }
                    }.value

                    pipelineStatus.filterEnd(streamName.toString())

                    parsedMessage.also {
                        it.info.startFilter = timeStart
                        it.info.endFilter = System.currentTimeMillis()
                        StreamWriter.setFilter(it.info)
                    }

                    if (filtered.finalFiltered) {
                        sendToChannel(
                            PipelineFilteredMessage(
                                parsedMessage,
                                filtered
                            )
                        )
                        pipelineStatus.countFilterAccepted(streamName.toString())
                    } else {
                        pipelineStatus.countFilterDiscarded(streamName.toString())
                    }
                    pipelineStatus.filterSendDownstream(streamName.toString())
                } else {
                    sendToChannel(parsedMessage)
                }
            }
        }
    }

    companion object {
        val logger = KotlinLogging.logger {}
        private val COUNTER = AtomicLong()
    }
}
