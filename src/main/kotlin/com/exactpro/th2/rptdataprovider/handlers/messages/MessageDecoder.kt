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
import com.exactpro.th2.rptdataprovider.entities.internal.MessagePipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.internal.ParsedMessage
import com.exactpro.th2.rptdataprovider.entities.internal.RawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import com.exactpro.th2.rptdataprovider.handlers.BatchPair
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

class MessageDecoder(
    private val context: Context,
    private val searchRequest: SseMessageSearchRequest,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent<MessagePipelineStepObject, RawBatch>
) : PipelineComponent<RawBatch, ParsedMessage>(externalScope, previousComponent) {


    private val maxBufferSize = 2
    private val elementBuffer = Channel<Deferred<BatchPair<RawBatch, ParsedMessageBatch?>>>(maxBufferSize)


    private suspend fun parse(rawBatch: RawBatch): BatchPair<RawBatch, ParsedMessageBatch?> {
        val parsedBatch = rawBatch.payload?.let {
            context.messageProducer.fromRawMessage(it.messageBatch, searchRequest)
        }
        return BatchPair(rawBatch, parsedBatch)
    }


    private suspend fun tryToLoadBuffer(coroutineScope: CoroutineScope) {
        while (coroutineScope.isActive) {
            val rawBatch = previousComponent!!.pollMessage()
            elementBuffer.send(coroutineScope.async { parse(rawBatch) })
        }
    }


    override suspend fun processMessage() {
        coroutineScope {
            launch { tryToLoadBuffer(this) }
            while (isActive) {
                val (rawBatch, parsedBatch) = elementBuffer.receive().await()
                if (parsedBatch != null && rawBatch.payload?.messages != null) {
                    rawBatch.payload.messages.forEach {
                        val parsedMessage = ParsedMessage(
                            rawBatch.streamEmpty,
                            parsedBatch.batch[it.id],
                            rawBatch.lastProcessedId,
                            rawBatch.lastScannedTime
                        )
                        sendToChannel(parsedMessage)
                    }
                } else {
                    sendToChannel(ParsedMessage(
                        rawBatch.streamEmpty,
                        null,
                        rawBatch.lastProcessedId,
                        rawBatch.lastScannedTime
                    ))
                }
            }
        }
    }
}