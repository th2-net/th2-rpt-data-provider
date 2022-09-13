package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.exceptions.CodecResponseException
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineDecodedBatch
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineParsedMessage
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.producers.EventBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class MessageBatchUnpacker(
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
    constructor(
        pipelineComponent: MessageBatchDecoder,
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


    private val sendInformationEvents = context.configuration.sendInformationEvents.value.toBoolean()
    private val useStrictMode = context.configuration.useStrictMode.value.toBoolean()


    private fun buildEventName(status: EventStatus): String {
        return buildString {
            if (status == EventStatus.SUCCESS) {
                append("Process message batch")
            } else {
                append("Failed message batch")
            }
        }
    }

    private suspend fun sendBatchInfoEvent(requests: Collection<MessageWrapper>, status: EventStatus) {
        if (sendInformationEvents && pipelineStatus.requestEvent?.id != null) {
            val messagesCount = requests.size
            val messagesSize = requests.sumBy { it.rawMessage.body.size() }

            val name = buildEventName(status)

            val event = EventBuilder()
                .parentId(pipelineStatus.requestEvent.id)
                .attachedIds(requests.map { it.id })
                .type("Batch information event")
                .name("${name}. Messages size: ${messagesSize}b, count: $messagesCount")
                .status(status)
                .build()

            context.rabbitMqService.storeEvent(event)
        }
    }


    private suspend fun badResponse(
        messages: Collection<MessageWrapper>,
        responses: List<MessageGroup>?
    ): List<Pair<MessageWrapper, MessageGroup?>> {

        sendBatchInfoEvent(messages, EventStatus.FAILED)

        val errorMessage = """"codec response is null 
                    | (stream=${streamName} 
                    | firstRequestId=${messages.first().id.index}
                    | lastRequestId=${messages.last().id.index} 
                    | requestSize=${messages.size})
                    | responseSize=${responses?.size ?: 0})"""
            .trimMargin().replace("\n", " ")

        return if (!useStrictMode) {
            logger.warn { errorMessage }
            messages.map { Pair(it, null) }
        } else {
            throw CodecResponseException(errorMessage)
        }
    }

    private suspend fun goodResponse(
        messages: Collection<MessageWrapper>,
        responses: List<MessageGroup>
    ): List<Pair<MessageWrapper, MessageGroup?>> {

        val requestsToResponse = messages.zip(responses)

        sendBatchInfoEvent(messages, EventStatus.SUCCESS)

        return if (!useStrictMode) {
            requestsToResponse.map { (rawMessage, response) ->
                if (response.messagesList.firstOrNull()?.hasMessage() == true) {
                    rawMessage to response
                } else {
                    rawMessage to null
                }
            }
        } else {
            val notParsed = mutableListOf<StoredMessageId>()
            val requestsToMessage = requestsToResponse.mapNotNull { (rawMessage, response) ->
                val message = response.messagesList.firstOrNull()
                if (message?.hasMessage() == true) {
                    rawMessage to response
                } else {
                    message?.let { notParsed.add(rawMessage.id) }
                    null
                }
            }

            if (notParsed.isNotEmpty()) {
                throw CodecResponseException(
                    """codec dont parsed all messages
                    | (stream=${streamName} 
                    | firstRequestId=${messages.first().id.index}
                    | lastRequestId=${messages.last().id.index}
                    | notParsedMessagesId=$notParsed
                """.trimMargin().replace("\n", " ")
                )
            }

            requestsToMessage
        }
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineDecodedBatch) {

            pipelineStatus.unpackStart(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            val requests = pipelineMessage.storedBatchWrapper.trimmedMessages

            val responses = measureTimedValue {
                pipelineMessage.codecResponse.protobufParsedMessageBatch.await()?.also {
                    pipelineMessage.info.codecResponse = it.responseTime
                    StreamWriter.setDecodeCodec(pipelineMessage.info)
                }
            }.also {
                logger.debug {
                    "awaited codec response for ${it.duration.inMilliseconds}ms (stream=${streamName} firstRequestId=${requests.first().id.index} lastRequestId=${requests.last().id.index} requestSize=${requests.size} responseSize=${it.value?.messageGroupBatch?.groupsList?.size})"
                }

            }.value?.messageGroupBatch?.groupsList

            pipelineMessage.info.endParseMessage = System.currentTimeMillis()
            StreamWriter.setDecodeAll(pipelineMessage.info)
            val requestsAndResponses =
                if (responses != null && requests.size == responses.size) {
                    goodResponse(requests, responses)
                } else {
                    badResponse(requests, responses)
                }

            val result = measureTimedValue {
                requestsAndResponses.map { (rawMessage, response) ->
                    PipelineParsedMessage(
                        pipelineMessage,
                        Message(
                            rawMessage,

                            let {
                                if (response == null) {
                                    pipelineStatus.countParseReceivedFailed(streamName.toString())
                                }

                                pipelineStatus.countParseReceivedTotal(streamName.toString())

                                response?.messagesList?.map { BodyWrapper(it.message) }
                            },
                            emptySet(),
                            pipelineMessage.imageType
                        )
                    )
                }
            }

            val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

            logger.debug { "codec response unpacking took ${result.duration.inMilliseconds}ms (stream=${streamName.toString()} firstId=${messages.first().id.index} lastId=${messages.last().id.index} messages=${messages.size})" }

            pipelineMessage.info.buildMessage = result.duration.inMilliseconds.toLong()
            StreamWriter.setBuildMessage(pipelineMessage.info)

            pipelineStatus.unpackEnd(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            result.value.forEach { (sendToChannel(it)) }

            pipelineStatus.unpackSendDownstream(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            logger.debug { "unpacked responses are sent (stream=${streamName.toString()} firstId=${messages.first().id.index} lastId=${messages.last().id.index} messages=${result.value.size})" }

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
