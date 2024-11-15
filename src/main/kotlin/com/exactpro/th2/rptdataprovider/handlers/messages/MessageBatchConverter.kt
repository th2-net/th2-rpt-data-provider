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

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import com.exactpro.th2.rptdataprovider.TransportRawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.mappers.ProtoMessageMapper
import com.exactpro.th2.rptdataprovider.entities.mappers.TransportMessageMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.ProtoCodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportCodecBatchRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

abstract class MessageBatchConverter<B, G, RM, PM>(
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
    init {
        externalScope.launch {
            while (isActive) {
                processMessage()
            }
        }
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }

    constructor(
        pipelineComponent: MessageExtractor<B, G, RM, PM>,
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

    protected val included = searchRequest.includeProtocols
    protected val excluded = searchRequest.excludeProtocols

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineRawBatch) {

            pipelineStatus.convertStart(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            val timeStart = System.currentTimeMillis()

            logger.trace { "received raw batch (stream=$commonStreamName first-time=${pipelineMessage.storedBatchWrapper.batchFirstTime})" }

            val filteredMessages: List<MessageHolder<G, RM>> = pipelineMessage.storedBatchWrapper.trimmedMessages.convertAndFilter()

            val codecRequest = PipelineCodecRequest(
                pipelineMessage.streamEmpty,
                pipelineMessage.lastProcessedId,
                pipelineMessage.lastScannedTime,
                MessageBatchWrapper(pipelineMessage.storedBatchWrapper.batchFirstTime, filteredMessages.map(MessageHolder<G, RM>::messageWrapper)),
                createCodecBatchRequest(filteredMessages),
                info = pipelineMessage.info.also {
                    it.startConvert = timeStart
                    it.endConvert = System.currentTimeMillis()
                    StreamWriter.setConvert(it)
                }
            )

            pipelineStatus.convertEnd(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            if (codecRequest.codecRequest.groupsCount > 0) {
                sendToChannel(codecRequest)
                logger.trace { "converted batch is sent downstream (stream=$commonStreamName first-time=${codecRequest.storedBatchWrapper.batchFirstTime} requestHash=${codecRequest.codecRequest.requestHash})" }
            } else {
                logger.trace { "converted batch is discarded because it has no messages (stream=$commonStreamName first-time=${pipelineMessage.storedBatchWrapper.batchFirstTime})" }
            }

            pipelineStatus.convertSendDownstream(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            pipelineStatus.countParsePrepared(
                commonStreamName.toString(),
                filteredMessages.size.toLong()
            )

        } else {
            sendToChannel(pipelineMessage)
        }
    }


    protected abstract fun Collection<StoredMessage>.convertAndFilter(): List<MessageHolder<G, RM>>
    protected abstract fun createCodecBatchRequest(filteredMessages: List<MessageHolder<G, RM>>):  CodecBatchRequest<B, G, PM>
}

class MessageHolder<G, RM>(
    val messageGroup: G,
    val messageWrapper: MessageWrapper<RM>
) {
    operator fun component1(): G = messageGroup
    operator fun component2(): MessageWrapper<RM> = messageWrapper
}

class ProtoMessageBatchConverter(
    pipelineComponent: MessageExtractor<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>,
    pipelineStatus: PipelineStatus
) : MessageBatchConverter<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>(
    pipelineComponent,
    pipelineComponent.context.configuration.messageConverterOutputBatchBuffer.value.toInt(),
    pipelineStatus
) {

    override fun Collection<StoredMessage>.convertAndFilter(): List<MessageHolder<MessageGroup, RawMessage>> = asSequence().map { message ->
        //FIXME: pass session group
        val messageWrapper = MessageWrapper(message, "", ProtoMessageMapper.storedMessageToRawProto(message))

        MessageHolder(ProtoMessageGroup.newBuilder().addMessages(
            AnyMessage.newBuilder()
                .setRawMessage(messageWrapper.rawMessage)
                .build()
        ).build(), messageWrapper)
    }.filter { (messages, _) ->
        val message = messages.messagesList.firstOrNull()?.message
        val protocol = message?.metadata?.protocol

        ((included.isNullOrEmpty() || included.contains(protocol))
                && (excluded.isNullOrEmpty() || !excluded.contains(protocol)))
            .also {
                logger.trace { "message ${message?.sequence} has protocol $protocol (matchesProtocolFilter=${it}) (stream=$commonStreamName)" }
            }
    }.toList()

    override fun createCodecBatchRequest(filteredMessages: List<MessageHolder<ProtoMessageGroup, ProtoRawMessage>>): ProtoCodecBatchRequest {
        return ProtoCodecBatchRequest(
            MessageGroupBatch
                .newBuilder()
                .addAllGroups(filteredMessages.map(MessageHolder<ProtoMessageGroup, ProtoRawMessage>::messageGroup))
                .build(),
            commonStreamName.toString()
        )
    }
}

class TransportMessageBatchConverter(
    pipelineComponent: MessageExtractor<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>,
    pipelineStatus: PipelineStatus
) : MessageBatchConverter<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>(
    pipelineComponent,
    pipelineComponent.context.configuration.messageConverterOutputBatchBuffer.value.toInt(),
    pipelineStatus
) {

    override fun Collection<StoredMessage>.convertAndFilter(): List<MessageHolder<TransportMessageGroup, TransportRawMessage>> = asSequence().map { message ->
        val messageWrapper =
            MessageWrapper(message, "", TransportMessageMapper.storedMessageToRawTransport(message))

        MessageHolder(messageWrapper.rawMessage.toGroup(), messageWrapper)
    }.filter { (messages, _) ->
        val message = messages.messages.firstOrNull()
        val protocol = message?.protocol

        ((included.isNullOrEmpty() || included.contains(protocol))
                && (excluded.isNullOrEmpty() || !excluded.contains(protocol)))
            .also {
                logger.trace { "message ${message?.id?.sequence} has protocol $protocol (matchesProtocolFilter=$it) (stream=$commonStreamName)" }
            }
    }.toList()

    override fun createCodecBatchRequest(filteredMessages: List<MessageHolder<TransportMessageGroup, TransportRawMessage>>): TransportCodecBatchRequest {
        val firstMessageWrapper = filteredMessages.first().messageWrapper
        return TransportCodecBatchRequest(
            GroupBatch.builder()
                .setBook(firstMessageWrapper.book)
                .setSessionGroup(firstMessageWrapper.sessionGroup)
                .setGroups(filteredMessages.map(MessageHolder<TransportMessageGroup, TransportRawMessage>::messageGroup))
                .build(),
            commonStreamName.toString()
        )
    }
}