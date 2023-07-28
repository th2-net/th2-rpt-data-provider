/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.*

class MessageFilter<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    searchRequest: SseMessageSearchRequest<RM, PM>,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent<B, G, RM, PM>?,
    messageFlowCapacity: Int
) : PipelineComponent<B, G, RM, PM>(context, searchRequest, externalScope, streamName, previousComponent, messageFlowCapacity) {

    private val sendEmptyDelay: Long = context.configuration.sendEmptyDelay.value.toLong()
    @Volatile
    private var lastScannedObject: PipelineStepObject? = null


    init {
        externalScope.launch {
            processMessage()
        }
    }


    constructor(
        pipelineComponent: MessageBatchUnpacker<B, G, RM, PM>,
        messageFlowCapacity: Int
    ) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.streamName,
        pipelineComponent.externalScope,
        pipelineComponent,
        messageFlowCapacity
    )

    private fun updateState(parsedMessage: PipelineParsedMessage<*, *>) {
        lastScannedObject = parsedMessage
        processedMessagesCounter++
    }


    private fun applyFilter(parsedMessage: Message<RM, PM>): MessageWithMetadata<RM, PM> {
        return MessageWithMetadata(parsedMessage).apply {
            finalFiltered = searchRequest.filterPredicate.apply(this)
        }
    }


    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            lastScannedObject?.let {
                sendToChannel(EmptyPipelineObject(it))
            }
            delay(sendEmptyDelay)
        }
    }


    override suspend fun processMessage() {
        coroutineScope {
            launch { emptySender(this) }
            while (isActive) {
                val parsedMessage = previousComponent!!.pollMessage()
                if (parsedMessage is PipelineParsedMessage<*, *>) {

                    updateState(parsedMessage)

                    val filtered = applyFilter(parsedMessage.payload as Message<RM, PM>)

                    if (filtered.finalFiltered) {
                        sendToChannel(PipelineFilteredMessage(parsedMessage, filtered))
                    }
                } else {
                    sendToChannel(parsedMessage)
                }
            }
        }
    }
}