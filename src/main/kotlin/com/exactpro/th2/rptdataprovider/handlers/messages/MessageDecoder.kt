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

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineParsedMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.*

@InternalCoroutinesApi
class MessageDecoder(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent?,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent(
    previousComponent?.startId,
    context,
    searchRequest,
    externalScope,
    streamName,
    previousComponent,
    messageFlowCapacity
) {
    init {
        externalScope.launch {
            processMessage()
        }
    }

    constructor(
        pipelineComponent: MessageContinuousStream,
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


    private fun <T> sublistOrEmpty(elements: List<T>, index: Int?): List<T> {
        return if (index == null) {
            emptyList()
        } else {
            elements.subList(index, elements.size)
        }
    }


    @InternalCoroutinesApi
    override suspend fun processMessage() {
        coroutineScope {

            while (isActive) {
                val rawBatch = previousComponent!!.pollMessage()

                if (rawBatch is PipelineRawBatchData) {

                    val buildersBatch = context.messageProducer.messageBatchToBuilders(rawBatch.payload)

                    if (buildersBatch.isImages) {
                        val inRangeBuilders = sublistOrEmpty(buildersBatch.builders, rawBatch.payload.firstIndexInRange)

                        inRangeBuilders.forEach { builder ->
                            val message = builder.build()
                            sendToChannel(PipelineParsedMessage(rawBatch, message))
                        }

                    } else {

                        pipelineStatus.countParseRequested(streamName.toString())

                        context.messageProducer.parseMessages(buildersBatch, this) {
                            launch {
                                buildersBatch.builders

                                    //FIXME: Builder should be self-sufficient. This is not a good idea.
                                    .find { builder ->
                                        builder.storedMessage.id.index == it.firstOrNull()?.id?.sequence
                                    }

                                    ?.let { builder ->
                                        builder.parsedMessageGroup(it)
                                        val message = builder.build()

                                        pipelineStatus.countParseReceived(streamName.toString())

                                        sendToChannel(PipelineParsedMessage(rawBatch, message))
                                    }
                            }
                        }
                    }
                } else {
                    sendToChannel(rawBatch)
                }
            }
        }
    }
}
