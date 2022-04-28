package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineCodecRequest
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import io.prometheus.client.Gauge
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
        batchConverterBufferSize.set(messageFlowCapacity.toDouble())
        externalScope.launch {
            while (isActive) {
                batchConverterBufferState
                    .labels(*listOf(streamName.toString()).toTypedArray())
                    .set(bufferState.toDouble())
                processMessage()
            }
        }
    }

    companion object {
        val logger = KotlinLogging.logger { }
        val batchConverterBufferSize: Gauge =
            Gauge.build("th2_message_bath_converter_buffer_size", "Batch converter buffer size ")
                .labelNames(*listOf("stream").toTypedArray())
                .register()
        val batchConverterBufferState: Gauge =
            Gauge.build("th2_message_bath_converter_buffer_size_state", "Batch converter buffer state")
                .labelNames(*listOf("stream").toTypedArray())
                .register()
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

            val timeStart = System.currentTimeMillis()

            logger.trace { "received raw batch (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.batchId})" }

            val filteredMessages = pipelineMessage.storedBatchWrapper.trimmedMessages
                .map {
                    val messageWrapper = MessageWrapper(it, RawMessage.parseFrom(it.content))

                    MessageGroup.newBuilder().addMessages(
                        AnyMessage.newBuilder()
                            .setRawMessage(messageWrapper.rawMessage)
                            .build()
                    ).build() to messageWrapper
                }
                .filter { (messages, _) ->
                    val message = messages.messagesList.firstOrNull()?.message
                    val protocol = message?.metadata?.protocol

                    ((included.isNullOrEmpty() || included.contains(protocol))
                            && (excluded.isNullOrEmpty() || !excluded.contains(protocol)))
                        .also {
                            logger.trace { "message ${message?.sequence} has protocol $protocol (matchesProtocolFilter=${it}) (stream=${streamName.toString()} batchId=${pipelineMessage.storedBatchWrapper.batchId})" }
                        }
                }


            val codecRequest = PipelineCodecRequest(
                pipelineMessage.streamEmpty,
                pipelineMessage.lastProcessedId,
                pipelineMessage.lastScannedTime,
                MessageBatchWrapper(pipelineMessage.storedBatchWrapper.batchId, filteredMessages.map { it.second }),
                CodecBatchRequest(
                    MessageGroupBatch
                        .newBuilder()
                        .addAllGroups(filteredMessages.map { it.first })
                        .build(),
                    streamName.toString()
                ),
                info = pipelineMessage.info.also {
                    it.startConvert = timeStart
                    it.endConvert = System.currentTimeMillis()
                    StreamWriter.setConvert(it)
                }
            )

            pipelineStatus.convertEnd(
                streamName.toString(),
                pipelineMessage.storedBatchWrapper.trimmedMessages.size.toLong()
            )

            if (codecRequest.codecRequest.protobufRawMessageBatch.groupsCount > 0) {
                sendToChannel(codecRequest)
                logger.trace { "converted batch is sent downstream (stream=${streamName.toString()} id=${codecRequest.storedBatchWrapper.batchId} requestHash=${codecRequest.codecRequest.requestHash})" }
            } else {
                logger.trace { "converted batch is discarded because it has no messages (stream=${streamName.toString()} id=${pipelineMessage.storedBatchWrapper.batchId})" }
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
