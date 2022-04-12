package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineDecodedBatch
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.getProtocolField
import com.exactpro.th2.rptdataprovider.entities.internal.ProtoProtocolInfo.isImage
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

        if (pipelineMessage is PipelineCodecRequest) {

            val protocol = getProtocolField(pipelineMessage.codecRequest.protobufRawMessageBatch)

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
                logger.trace { "received converted batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.batchId} requestHash=${pipelineMessage.codecRequest.requestHash})" }

                pipelineStatus.decodeStart(
                    streamName.toString(),
                    pipelineMessage.codecRequest.protobufRawMessageBatch.groupsCount.toLong()
                )

                val result = PipelineDecodedBatch(
                    pipelineMessage.also {
                        it.info.startParseMessage = System.currentTimeMillis()
                    },
                    context.rabbitMqService.sendToCodec(pipelineMessage.codecRequest),
                    protocol
                )
                pipelineStatus.decodeEnd(
                    streamName.toString(),
                    pipelineMessage.codecRequest.protobufRawMessageBatch.groupsCount.toLong()
                )
                sendToChannel(result)
                pipelineStatus.decodeSendDownstream(
                    streamName.toString(),
                    pipelineMessage.codecRequest.protobufRawMessageBatch.groupsCount.toLong()
                )

                logger.trace { "decoded batch is sent downstream (stream=${streamName.toString()} id=${result.storedBatchWrapper.batchId} requestHash=${pipelineMessage.codecRequest.requestHash})" }
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
