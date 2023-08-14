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

import com.exactpro.cradle.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import com.exactpro.th2.rptdataprovider.TransportRawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.StreamName
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import kotlinx.coroutines.CoroutineScope

abstract class ChainBuilder<B, G, RM, PM>(
    private val context: Context<B, G, RM, PM>,
    private val request: SseMessageSearchRequest<RM, PM>,
    private val externalScope: CoroutineScope,
    protected val pipelineStatus: PipelineStatus
) {
    fun buildChain(): StreamMerger<B, G, RM, PM> {
        val streamNames =
            request.stream.flatMap { stream -> Direction.values().map { StreamName(stream, it, request.bookId) } }


        pipelineStatus.addStreams(streamNames.map { it.toString() })

        val dataStreams = streamNames.map { streamName ->

            val messageExtractor = MessageExtractor(
                context,
                request,
                streamName,
                externalScope,
                context.configuration.messageExtractorOutputBatchBuffer.value.toInt(),
                pipelineStatus
            )

            val messageBatchConverter = createConverter(messageExtractor)

            val messageBatchDecoder = MessageBatchDecoder(
                messageBatchConverter,
                context.configuration.messageDecoderOutputBatchBuffer.value.toInt(),
                pipelineStatus
            )

            val messageBatchUnpacker = createUnpacker(messageBatchDecoder)

            MessageFilter(
                messageBatchUnpacker,
                context.configuration.messageFilterOutputMessageBuffer.value.toInt()
            )
        }

        return StreamMerger(
            context,
            request,
            externalScope,
            dataStreams,
            context.configuration.messageMergerOutputMessageBuffer.value.toInt(),
            pipelineStatus
        )
    }

    protected abstract fun createUnpacker(decoder: MessageBatchDecoder<B, G, RM, PM>): MessageBatchUnpacker<B, G, RM, PM>

    protected abstract fun createConverter(extractor: MessageExtractor<B, G, RM, PM>): MessageBatchConverter<B, G, RM, PM>
}

class ProtoChainBuilder(
    context: Context<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>,
    request: SseMessageSearchRequest<ProtoRawMessage, Message>,
    externalScope: CoroutineScope,
    pipelineStatus: PipelineStatus
): ChainBuilder<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>(
    context, request, externalScope, pipelineStatus
) {
    override fun createConverter(extractor: MessageExtractor<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>) =
        ProtoMessageBatchConverter(
            extractor,
            pipelineStatus
        )

    override fun createUnpacker(decoder: MessageBatchDecoder<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>) =
        ProtoMessageBatchUnpacker(
            decoder,
            pipelineStatus
        )
}

class TransportChainBuilder(
    context: Context<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>,
    request: SseMessageSearchRequest<TransportRawMessage, ParsedMessage>,
    externalScope: CoroutineScope,
    pipelineStatus: PipelineStatus
): ChainBuilder<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>(
    context, request, externalScope, pipelineStatus
) {
    override fun createConverter(extractor: MessageExtractor<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>) =
        TransportMessageBatchConverter(
            extractor,
            pipelineStatus
        )

    override fun createUnpacker(decoder: MessageBatchDecoder<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>) =
        TransportMessageBatchUnpacker(
            decoder,
            pipelineStatus
        )
}
