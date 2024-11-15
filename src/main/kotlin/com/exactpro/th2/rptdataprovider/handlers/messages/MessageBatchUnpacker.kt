/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.exceptions.CodecResponseException
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

abstract class MessageBatchUnpacker<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    searchRequest: SseMessageSearchRequest<RM, PM>,
    streamName: CommonStreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent<B, G, RM, PM>?,
    messageFlowCapacity: Int,
    protected val pipelineStatus: PipelineStatus
) : PipelineComponent<B, G, RM, PM>(
    context,
    searchRequest,
    externalScope,
    streamName,
    previousComponent,
    messageFlowCapacity
) {
    constructor(
        pipelineComponent: MessageBatchDecoder<B, G, RM, PM>,
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


    protected val useStrictMode = context.configuration.useStrictMode.value.toBoolean()


    private fun badResponse(
        requests: Collection<MessageWrapper<RM>>,
        responses: List<*>?,
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>
    ): List<Pair<MessageWrapper<RM>, G?>> {

        val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

        val errorMessage = """"codec response is null 
                    | (stream=${commonStreamName} 
                    | firstRequestId=${messages.first().id.sequence}
                    | lastRequestId=${messages.last().id.sequence} 
                    | requestSize=${messages.size})
                    | responseSize=${responses?.size ?: 0})"""
            .trimMargin().replace("\n", " ")

        return if (!useStrictMode) {
            logger.warn { errorMessage }
            requests.map { Pair(it, null) }
        } else {
            throw CodecResponseException(errorMessage)
        }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineDecodedBatch<*, *, *, *>) {

            pipelineStatus.unpackStart(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            @Suppress("UNCHECKED_CAST") val requests = pipelineMessage.storedBatchWrapper.trimmedMessages as Collection<MessageWrapper<RM>>

            val responses = measureTimedValue {
                pipelineMessage.codecResponse.parsedMessageBatch.await()?.also {
                    pipelineMessage.info.codecResponse = it.responseTime
                    StreamWriter.setDecodeCodec(pipelineMessage.info)
                }
            }.also {
                logger.debug {
                    "awaited codec response for ${it.duration.toDouble(DurationUnit.MILLISECONDS)}ms (stream=${commonStreamName} firstRequestId=${requests.first().id.sequence} lastRequestId=${requests.last().id.sequence} requestSize=${requests.size} responseSize=${it.value?.groupCount})"
                }
            }.value?.groups

            pipelineMessage.info.endParseMessage = System.currentTimeMillis()
            StreamWriter.setDecodeAll(pipelineMessage.info)
            val requestsAndResponses: List<Pair<MessageWrapper<RM>, G?>> =
                if (responses != null && requests.size == responses.size) {
                    goodResponse(requests, responses, pipelineMessage)
                } else {
                    badResponse(requests, responses, pipelineMessage)
                }

            val result = measureTimedValue {
                requestsAndResponses.map { (rawMessage, response) ->
                    createPipelineParsedMessage(pipelineMessage, rawMessage, response)
                }
            }

            val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

            logger.debug { "codec response unpacking took ${result.duration.toDouble(DurationUnit.MILLISECONDS)}ms (stream=$commonStreamName firstId=${messages.first().id.sequence} lastId=${messages.last().id.sequence} messages=${messages.size})" }

            pipelineMessage.info.buildMessage = result.duration.toDouble(DurationUnit.MILLISECONDS).toLong()
            StreamWriter.setBuildMessage(pipelineMessage.info)

            pipelineStatus.unpackEnd(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            result.value.forEach { (sendToChannel(it)) }

            pipelineStatus.unpackSendDownstream(
                commonStreamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            logger.debug { "unpacked responses are sent (stream=$commonStreamName firstId=${messages.first().id.sequence} lastId=${messages.last().id.sequence} messages=${result.value.size})" }

        } else {
            sendToChannel(pipelineMessage)
        }
    }

    protected abstract fun goodResponse(
        requests: Collection<MessageWrapper<RM>>,
        responses: List<*>,
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>
    ): List<Pair<MessageWrapper<RM>, G?>>

    protected abstract fun createPipelineParsedMessage(
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>,
        rawMessageWrapper: MessageWrapper<RM>,
        response: G?
    ): PipelineParsedMessage<RM, PM>
}

class ProtoMessageBatchUnpacker(
    pipelineComponent: MessageBatchDecoder<MessageGroupBatch, MessageGroup, RawMessage, com.exactpro.th2.common.grpc.Message>,
    pipelineStatus: PipelineStatus
) : MessageBatchUnpacker<MessageGroupBatch, MessageGroup, RawMessage, com.exactpro.th2.common.grpc.Message>(
    pipelineComponent,
    pipelineComponent.context.configuration.messageConverterOutputBatchBuffer.value.toInt(),
    pipelineStatus
) {

    override fun goodResponse(
        requests: Collection<MessageWrapper<RawMessage>>,
        responses: List<*>,
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>
    ): List<Pair<MessageWrapper<RawMessage>, MessageGroup?>> {

        val requestsToResponse: List<Pair<MessageWrapper<RawMessage>, Any?>> = requests.zip(responses)

        return if (!useStrictMode) {
            requestsToResponse.map { (rawMessage, response) ->
                require(response is MessageGroup) {
                    "Response type mismatch, expected: ${MessageGroup::class.java}, actual: ${response?.let { it::class.java }}"
                }
                if (response.messagesList.firstOrNull()?.hasMessage() == true) {
                    rawMessage to response
                } else {
                    rawMessage to null
                }
            }
        } else {
            val notParsed = mutableListOf<StoredMessageId>()
            val requestsToMessage = requestsToResponse.mapNotNull { (rawMessage, response) ->
                require(response is MessageGroup) {
                    "Response type mismatch, expected: ${MessageGroup::class.java}, actual: ${response?.let { it::class.java }}"
                }
                val message = response.messagesList.firstOrNull()
                if (message?.hasMessage() == true) {
                    rawMessage to response
                } else {
                    message?.let { notParsed.add(rawMessage.id) }
                    null
                }
            }

            if (notParsed.isNotEmpty()) {
                val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

                throw CodecResponseException(
                    """codec don't parsed all messages
                    | (stream=${commonStreamName} 
                    | firstRequestId=${messages.first().id.sequence}
                    | lastRequestId=${messages.last().id.sequence}
                    | notParsedMessagesId=$notParsed
                """.trimMargin().replace("\n", " ")
                )
            }

            requestsToMessage
        }
    }



    override fun createPipelineParsedMessage(
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>,
        rawMessageWrapper: MessageWrapper<RawMessage>,
        response: MessageGroup?
    ): PipelineParsedMessage<RawMessage, com.exactpro.th2.common.grpc.Message> {
        return PipelineParsedMessage(
            pipelineMessage,
            Message(
                rawMessageWrapper,

                let {
                    if (response == null) {
                        pipelineStatus.countParseReceivedFailed(commonStreamName.toString())
                    }

                    pipelineStatus.countParseReceivedTotal(commonStreamName.toString())

                    response?.messagesList?.map { ProtoBodyWrapper(it.message) }
                },
                emptySet(),
                pipelineMessage.imageType
            )
        )
    }
}

class TransportMessageBatchUnpacker(
    pipelineComponent: MessageBatchDecoder<GroupBatch, com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup, com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage, ParsedMessage>,
    pipelineStatus: PipelineStatus
) : MessageBatchUnpacker<GroupBatch, com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup, com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage, ParsedMessage>(
    pipelineComponent,
    pipelineComponent.context.configuration.messageConverterOutputBatchBuffer.value.toInt(),
    pipelineStatus
) {

    override fun goodResponse(
        requests: Collection<MessageWrapper<com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage>>,
        responses: List<*>,
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>
    ): List<Pair<MessageWrapper<com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage>, com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup?>> {

        val requestsToResponse: List<Pair<MessageWrapper<com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage>, Any?>> = requests.zip(responses)

        return if (!useStrictMode) {
            requestsToResponse.map { (rawMessage, response) ->
                require(response is com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup) {
                    "Response type mismatch, expected: ${com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup::class.java}, actual: ${response?.let { it::class.java }}"
                }
                if (response.messages.firstOrNull() is ParsedMessage) {
                    rawMessage to response
                } else {
                    rawMessage to null
                }
            }
        } else {
            val notParsed = mutableListOf<StoredMessageId>()
            val requestsToMessage = requestsToResponse.mapNotNull { (rawMessage, response) ->
                require(response is com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup) {
                    "Response type mismatch, expected: ${com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup::class.java}, actual: ${response?.let { it::class.java }}"
                }
                val message = response.messages.firstOrNull()
                if (message is ParsedMessage) {
                    rawMessage to response
                } else {
                    message?.let { notParsed.add(rawMessage.id) }
                    null
                }
            }

            if (notParsed.isNotEmpty()) {
                val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

                throw CodecResponseException(
                    """codec don't parsed all messages
                    | (stream=${commonStreamName} 
                    | firstRequestId=${messages.first().id.sequence}
                    | lastRequestId=${messages.last().id.sequence}
                    | notParsedMessagesId=$notParsed
                """.trimMargin().replace("\n", " ")
                )
            }

            requestsToMessage
        }
    }



    override fun createPipelineParsedMessage(
        pipelineMessage: PipelineDecodedBatch<*, *, *, *>,
        rawMessageWrapper: MessageWrapper<com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage>,
        response: com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup?
    ): PipelineParsedMessage<com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage, ParsedMessage> {
        return PipelineParsedMessage(
            pipelineMessage,
            Message(
                rawMessageWrapper,

                let {
                    if (response == null) {
                        pipelineStatus.countParseReceivedFailed(commonStreamName.toString())
                    }

                    pipelineStatus.countParseReceivedTotal(commonStreamName.toString())

                    response?.messages?.map { TransportBodyWrapper((it as ParsedMessage), rawMessageWrapper.book, rawMessageWrapper.sessionAlias) }
                },
                emptySet(),
                pipelineMessage.imageType
            )
        )
    }
}