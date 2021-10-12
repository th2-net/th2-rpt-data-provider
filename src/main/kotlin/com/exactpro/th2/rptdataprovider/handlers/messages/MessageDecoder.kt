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
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.producers.BuildersBatch
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.rabbitmq.MessageRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.system.measureTimeMillis
import kotlin.time.measureTime

class MessageDecoder(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent?
) : PipelineComponent(context, searchRequest, externalScope, streamName, previousComponent) {

    constructor(pipelineComponent: MessageContinuousStream) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.streamName,
        pipelineComponent.externalScope,
        pipelineComponent
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
        builders: List<Message.Builder>,
        messageRequests: List<MessageRequest?>,
        rawBatch: PipelineRawBatchData
    ) {
        val inRangeBuilders = sublistOrEmpty(builders, rawBatch.payload.firstIndexInRange)
        val inRangeMessageRequests = sublistOrEmpty(messageRequests, rawBatch.payload.firstIndexInRange)

        setAttachedEvents(inRangeBuilders)

        inRangeBuilders.forEachIndexed { i, builder ->
            val messageRequest = inRangeMessageRequests[i]
            builder.parsedMessage(messageRequest?.get())
            val message = builder.build()
            sendToChannel(PipelineParsedMessage(rawBatch, message))
        }
    }


    private suspend fun getMessageRequests(
        rawBatch: PipelineRawBatchData,
        buildersBatch: BuildersBatch,
        coroutineScope: CoroutineScope
    ): List<MessageRequest?> {
        return context.messageProducer.parseMessages(
            rawBatch.payload,
            buildersBatch.rawMessages,
            coroutineScope
        )
    }


    override suspend fun processMessage() {
        coroutineScope {
            while (isActive) {
                val rawBatch = previousComponent!!.pollMessage()

                if (rawBatch is PipelineRawBatchData) {

                    val buildersBatch = createMessageBuilders(rawBatch.payload)

                    if (buildersBatch.isImages) {
                        sendImages(buildersBatch.builders, rawBatch)
                    } else {
                        val messageRequests = getMessageRequests(rawBatch, buildersBatch, this)
                        sendParsedMessages(buildersBatch.builders, messageRequests, rawBatch)
                    }
                } else {
                    sendToChannel(rawBatch)
                }
            }
        }
    }
}