package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineDecodedBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchResponse
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging

class MessageBatchDecoder(
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
        pipelineComponent: MessageBatchConverter,
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

    private val messageProtocolDescriptor =
        RawMessage.getDescriptor()
            .findFieldByName("metadata")
            .messageType
            .findFieldByName("protocol")

    private val TYPE_IMAGE = "image"

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

    private fun getProtocolField(rawMessageBatch: RawMessageBatch?): String? {
        val parsedRawMessage = rawMessageBatch?.messagesList?.first()
        return try {
            parsedRawMessage?.metadata?.getField(messageProtocolDescriptor).toString()
        } catch (e: Exception) {
            logger.error(e) { "Field: '${messageProtocolDescriptor.name}' does not exist in message: $parsedRawMessage " }
            null
        }
    }

    private fun isImage(protocolName: String?): Boolean {
        return protocolName?.contains(TYPE_IMAGE) ?: false
    }

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineCodecRequest) {

            val protocol = getProtocolField(pipelineMessage.codecRequest.protobufRawMessageBatch)

            if (isImage(protocol)) {
                sendToChannel(
                    PipelineDecodedBatch(
                        pipelineMessage,
                        CodecBatchResponse(CompletableDeferred(value = null)),
                        protocol
                    )
                )
            } else {
                sendToChannel(
                    PipelineDecodedBatch(
                        pipelineMessage,
                        context.rabbitMqService.sendToCodec(pipelineMessage.codecRequest)
                    )
                )
            }

            pipelineStatus.countParseRequested(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.count().toLong()
            )

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
