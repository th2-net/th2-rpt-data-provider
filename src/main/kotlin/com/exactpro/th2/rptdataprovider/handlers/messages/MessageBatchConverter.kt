package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.services.CodecBatchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging

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

            pipelineStatus.convertStart(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            logger.trace { "received raw batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id})" }

            val filteredMessages = pipelineMessage.storedBatchWrapper.trimmedMessages
                .map {
                    MessageGroup.newBuilder().addMessages(
                        AnyMessage.newBuilder()
                            .setRawMessage(RawMessage.parseFrom(it.content))
                            .build()
                    ).build() to it
                }
                .filter { (messages, _) ->
                    val message = messages.messagesList.firstOrNull()?.message
                    val protocol = message?.metadata?.protocol

                    ((included.isNullOrEmpty() || included.contains(protocol))
                            && (excluded.isNullOrEmpty() || !excluded.contains(protocol)))
                        .also {
                            logger.trace { "message ${message?.sequence} has protocol $protocol (matchesProtocolFilter=${it}) (stream=${streamName.toString()} batchId=${pipelineMessage.storedBatchWrapper.fullBatch.id})" }
                        }
                }


            val codecRequest = PipelineCodecRequest(
                pipelineMessage.streamEmpty,
                pipelineMessage.lastProcessedId,
                pipelineMessage.lastScannedTime,
                pipelineMessage.storedBatchWrapper.copy(trimmedMessages = filteredMessages.map { it.second }),
                CodecBatchRequest(
                    MessageGroupBatch
                        .newBuilder()
                        .addAllGroups(filteredMessages.map { it.first })
                        .build(),
                    streamName.toString()
                )
            )

            pipelineStatus.convertEnd(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            if (codecRequest.codecRequest.protobufRawMessageBatch.groupsCount > 0) {
                sendToChannel(codecRequest)
                logger.trace { "converted batch is sent downstream (stream=${streamName.toString()} id=${codecRequest.storedBatchWrapper.fullBatch.id} requestHash=${codecRequest.codecRequest.requestHash})" }
            } else {
                logger.trace { "converted batch is discarded because it has no messages (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.fullBatch.id})" }
            }

            pipelineStatus.convertSendDownstream(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            pipelineStatus.countParsePrepared(
                streamName.toString(),
                filteredMessages.size.toLong()
            )

        } else {
            sendToChannel(pipelineMessage)
        }
    }
}