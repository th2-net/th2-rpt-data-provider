package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

class MessageBatchConverter(
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
    init {
        externalScope.launch {
            while (isActive) {
                processMessage()
            }
        }
    }

    constructor(
        pipelineComponent: MessageExtractor, messageFlowCapacity: Int, pipelineStatus: PipelineStatus
    ) : this(
        pipelineComponent.context,
        pipelineComponent.searchRequest,
        pipelineComponent.streamName,
        pipelineComponent.externalScope,
        pipelineComponent,
        messageFlowCapacity,
        pipelineStatus
    )

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineRawBatch) {

            val codecRequest = PipelineCodecRequest(
                pipelineMessage.streamEmpty,
                pipelineMessage.lastProcessedId,
                pipelineMessage.lastScannedTime,
                pipelineMessage.storedBatchWrapper,

                CodecBatchRequest(
                    MessageGroupBatch
                        .newBuilder()
                        .addAllGroups(

                            pipelineMessage.storedBatchWrapper.trimmedMessages
                                .map {
                                    MessageGroup.newBuilder().addMessages(
                                        AnyMessage.newBuilder().setRawMessage(RawMessage.parseFrom(it.content)).build()
                                    ).build()
                                }

                        )
                        .build()
                )
            )

            sendToChannel(codecRequest)

            pipelineStatus.countParsePrepared(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.count().toLong()
            )

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
