/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineDecodedBatch
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.isImage
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

class MessageBatchDecoder<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    searchRequest: SseMessageSearchRequest<RM, PM>,
    streamName: CommonStreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent<B, G, RM, PM>?,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent<B, G, RM, PM>(
    context,
    searchRequest,
    externalScope,
    streamName,
    previousComponent,
    messageFlowCapacity
) {

    constructor(
        pipelineComponent: MessageBatchConverter<B, G, RM, PM>,
        messageFlowCapacity: Int,
        pipelineStatus: PipelineStatus
    ) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.commonStreamName,
        pipelineComponent.externalScope,
        pipelineComponent,
        messageFlowCapacity,
        pipelineStatus
    )



    companion object {
        private val logger = KotlinLogging.logger { }
    }

    init {
        externalScope.launch {
            while (isActive) {
                processMessage()
            }
        }
    }

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineCodecRequest<*, *, *, *>) {
            @Suppress("UNCHECKED_CAST")
            pipelineMessage as PipelineCodecRequest<B, G, RM, PM>

            val protocol = pipelineMessage.codecRequest.protocol
            if (isImage(protocol)) {
                sendToChannel(
                    PipelineDecodedBatch(
                        pipelineMessage.also {
                            it.info.startParseMessage = System.currentTimeMillis()
                        },
                        CodecBatchResponse(CompletableDeferred(value = null)),
                        protocol
                    )
                )
            } else {
                logger.trace { "received converted batch (stream=$commonStreamName first-time=${pipelineMessage.storedBatchWrapper.batchFirstTime} requestHash=${pipelineMessage.codecRequest.requestHash})" }

                pipelineStatus.decodeStart(
                    commonStreamName.toString(),
                    pipelineMessage.codecRequest.groupsCount.toLong()
                )

                val codecRequest:  CodecBatchRequest<B, G, PM> = pipelineMessage.codecRequest
                val result = PipelineDecodedBatch(
                    pipelineMessage.also {
                        it.info.startParseMessage = System.currentTimeMillis()
                    },
                    context.rabbitMqService.sendToCodec(codecRequest),
                    protocol
                )
                pipelineStatus.decodeEnd(
                    commonStreamName.toString(),
                    pipelineMessage.codecRequest.groupsCount.toLong()
                )
                sendToChannel(result)
                pipelineStatus.decodeSendDownstream(
                    commonStreamName.toString(),
                    pipelineMessage.codecRequest.groupsCount.toLong()
                )

                logger.trace { "decoded batch is sent downstream (stream=$commonStreamName first-time=${result.storedBatchWrapper.batchFirstTime} requestHash=${pipelineMessage.codecRequest.requestHash})" }
            }

            pipelineStatus.countParseRequested(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.count().toLong()
            )

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
