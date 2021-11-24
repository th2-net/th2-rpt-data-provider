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
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineParsedMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.producers.BuildersBatch
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import kotlinx.coroutines.*

@InternalCoroutinesApi
class MessageDecoder(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent?,
    messageFlowCapacity: Int
) : PipelineComponent(
    previousComponent?.startId,
    context,
    searchRequest,
    externalScope,
    streamName,
    previousComponent,
    messageFlowCapacity
) {

    private val batchMergeSize = context.configuration.rabbitMergedBatchSize.value.toLong()

    init {
        externalScope.launch {
            processMessage()
        }
    }

    constructor(pipelineComponent: MessageContinuousStream, messageFlowCapacity: Int) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.streamName,
        pipelineComponent.externalScope,
        pipelineComponent,
        messageFlowCapacity
    )

    private suspend fun createMessageBuilders(rawBatch: MessageBatchWrapper): BuildersBatch {
        return context.messageProducer.messageBatchToBuilders(rawBatch)
    }


    private suspend fun setAttachedEvents(builders: List<Message.Builder>) {
        if (searchRequest.attachedEvents) {
            context.messageProducer.attachEvents(builders)
        }
    }


    private fun <T> sublistOrEmpty(elements: List<T>, index: Int?): List<T> {
        return if (index == null) {
            emptyList()
        } else {
            elements.subList(index, elements.size)
        }
    }


    private suspend fun sendImages(builders: List<Message.Builder>, rawBatch: PipelineRawBatchData) {
        val inRangeBuilders = sublistOrEmpty(builders, rawBatch.payload.firstIndexInRange)

        setAttachedEvents(inRangeBuilders)

        inRangeBuilders.forEach { builder ->
            val message = builder.build()
            sendToChannel(PipelineParsedMessage(rawBatch, message))
        }
    }


    private suspend fun sendParsedMessages(
        buffer: MutableList<Pair<BuildersBatch, PipelineRawBatchData>>,
        messageRequests: List<List<MessageRequest?>>
    ) {
        buffer.mapIndexed { index, (buildersBatch, rawBatch) ->

            val inRangeBuilders = sublistOrEmpty(buildersBatch.builders, rawBatch.payload.firstIndexInRange)
            val inRangeMessageRequests = sublistOrEmpty(messageRequests[index], rawBatch.payload.firstIndexInRange)

            setAttachedEvents(inRangeBuilders)

            inRangeBuilders.forEachIndexed { i, builder ->
                val messageRequest = inRangeMessageRequests[i]
                builder.parsedMessage(messageRequest?.get())
                val message = builder.build()
                sendToChannel(PipelineParsedMessage(rawBatch, message))
            }
        }
    }


    @InternalCoroutinesApi
    private suspend fun getMessageRequests(
        rawBatch: List<BuildersBatch>,
        coroutineScope: CoroutineScope
    ): List<List<MessageRequest?>> {
        return context.messageProducer.parseMessages(rawBatch, coroutineScope)
    }


    @InternalCoroutinesApi
    override suspend fun processMessage() {
        coroutineScope {
            val buffer = mutableListOf<Pair<BuildersBatch, PipelineRawBatchData>>()
            var messagesInBuffer = 0L

            while (isActive) {
                val rawBatch = previousComponent!!.pollMessage()

                if (messagesInBuffer >= batchMergeSize || rawBatch is EmptyPipelineObject) {
                    getMessageRequests(buffer.map { it.first }, this).let {
                        sendParsedMessages(buffer, it)
                        messagesInBuffer = 0L
                        buffer.clear()
                    }
                }

                if (rawBatch is PipelineRawBatchData) {

                    val buildersBatch = createMessageBuilders(rawBatch.payload)

//                    if (buildersBatch.isImages) {
                    sendImages(buildersBatch.builders, rawBatch)
//                    } else {
//                        buffer.add(buildersBatch to rawBatch)
//                        messagesInBuffer += rawBatch.payload.messageBatch.messageCount
//                    }
                } else {
                    sendToChannel(rawBatch)
                }
            }
        }
    }
}
