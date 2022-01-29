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

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineDecodedBatch) {

            val responsesBySequence = pipelineMessage.codecResponse.protobufParsedMessageBatch.await()?.messagesList
                ?.groupBy { it.metadata.id.sequence }

            pipelineMessage.storedBatchWrapper.trimmedMessages.forEach { storedMessage ->
                sendToChannel(
                    PipelineParsedMessage(
                        pipelineMessage,
                        Message(
                            storedMessage,

                            let {
                                val response = responsesBySequence?.get(storedMessage.id.index)

                                if (response == null) {
                                    pipelineStatus.countParseReceivedFailed(streamName.toString())
                                }

                                pipelineStatus.countParseReceivedTotal(streamName.toString())

                                response?.map { BodyWrapper(it) }
                            },

                            storedMessage.content,
                            emptySet()
                        )
                    )
                )
            }

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
