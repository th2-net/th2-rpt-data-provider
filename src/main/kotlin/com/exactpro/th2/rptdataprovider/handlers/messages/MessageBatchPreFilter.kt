package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging

class MessageBatchPreFilter(
    context: Context,
    searchRequest: SseMessageSearchRequest,
    streamName: StreamName?,
    externalScope: CoroutineScope,
    previousComponent: PipelineComponent?,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent(
    context, searchRequest, externalScope, streamName, previousComponent, messageFlowCapacity
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

    private val included = searchRequest.includeProtocols
    private val excluded = searchRequest.excludeProtocols

    override suspend fun processMessage() {
        val pipelineMessage = previousComponent!!.pollMessage()

        if (pipelineMessage is PipelineRawBatch) {

            logger.trace { "received raw batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id})" }

            if (included == null && excluded == null) {
                sendToChannel(pipelineMessage)
            } else {

                val originalMessageCount = pipelineMessage.storedBatchWrapper.trimmedMessages.size

                val filteredMessages = pipelineMessage.storedBatchWrapper.trimmedMessages.filter { message ->
                    val protocol = message.metadata?.get("protocol")


                    ((included.isNullOrEmpty() || included.contains(protocol))
                            && (excluded.isNullOrEmpty() || !excluded.contains(protocol)))
                        .also {
                            logger.trace { "message ${message.id.index} has protocol $protocol matchesProtocolFilter=${it} (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id})" }
                        }
                }

                logger.trace { "raw batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id}) has ${filteredMessages.size} out of $originalMessageCount messages that match protocol filter (included=${included} excluded=${excluded})" }

                if (filteredMessages.isNotEmpty()) {
                    sendToChannel(
                        PipelineRawBatch(
                            pipelineMessage.streamEmpty,
                            pipelineMessage.lastProcessedId,
                            pipelineMessage.lastScannedTime,
                            MessageBatchWrapper(
                                pipelineMessage.storedBatchWrapper.fullBatch,
                                filteredMessages
                            )
                        )
                    )
                } else {
                    logger.trace { "discarding raw batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id}) because it has no messages" }
                }
            }

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}
