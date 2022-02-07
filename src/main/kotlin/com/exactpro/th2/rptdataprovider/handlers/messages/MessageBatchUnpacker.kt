package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineDecodedBatch
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineParsedMessage
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
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

    @OptIn(ExperimentalTime::class)
    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineDecodedBatch) {

            val requests = pipelineMessage.storedBatchWrapper.trimmedMessages

            val responses = measureTimedValue {
                pipelineMessage.codecResponse.protobufParsedMessageBatch.await()
            }.also {
                logger.debug {
                    "awaited codec response for ${it.duration.inMilliseconds}ms (stream=${streamName} firstRequestId=${requests.first().id.index} lastRequestId=${requests.last().id.index} requestSize=${requests.size} responseSize=${it.value?.groupsList?.size})"
                }
            }.value?.groupsList

            val requestsAndResponses =
                if (responses != null) {
                    requests.zip(responses).map { (rawMessage, response) ->
                        if (response.messagesList.firstOrNull()?.hasMessage() == true) {
                            rawMessage to response
                        } else {
                            rawMessage to null
                        }
                    }
                } else {
                    val messages = pipelineMessage.storedBatchWrapper.trimmedMessages
                    logger.warn { "codec response is null (stream=${streamName} firstRequestId=${messages.first().id.index} lastRequestId=${messages.last().id.index} requestSize=${messages.size} responseSize=${responses?.size ?: 0})" }
                    requests.map { Pair(it, null) }
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

                            rawMessage.content,
                            emptySet()
                        )
                    )
                }
            }

            val messages = pipelineMessage.storedBatchWrapper.trimmedMessages

            logger.debug { "codec response unpacking took ${result.duration.inMilliseconds}ms (stream=${streamName.toString()} firstId=${messages.first().id.index} lastId=${messages.last().id.index} messages=${messages.size})" }

            result.value.forEach { (sendToChannel(it)) }

            logger.debug { "unpacked responses are sent (stream=${streamName.toString()} firstId=${messages.first().id.index} lastId=${messages.last().id.index} messages=${result.value.size})" }

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
