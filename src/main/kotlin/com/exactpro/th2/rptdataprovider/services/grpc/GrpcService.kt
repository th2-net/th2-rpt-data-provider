package com.exactpro.th2.rptdataprovider.services.grpc

import com.exactpro.th2.codec.grpc.AsyncCodecService
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.services.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.CodecBatchResponse
import com.exactpro.th2.rptdataprovider.services.CodecRequestId
import com.exactpro.th2.rptdataprovider.services.DecoderService
import com.exactpro.th2.rptdataprovider.services.MessageGroupBatchWrapper
import com.exactpro.th2.rptdataprovider.services.PendingCodecBatchRequest
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class GrpcService(
    configuration: Configuration,
    grpcRouter: GrpcRouter
) : DecoderService {

    private val responseTimeout = configuration.codecResponseTimeout.value.toLong()
    private val pendingRequests = ConcurrentHashMap<CodecRequestId, PendingCodecBatchRequest>()
    private val usePinAttributes = configuration.codecUsePinAttributes.value.toBoolean()

    private val codecService = grpcRouter.getService(AsyncCodecService::class.java)
    private val maximumPendingRequests = configuration.codecPendingBatchLimit.value.toInt()

    private val requestSenderScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecRequestThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    private val callbackScope = CoroutineScope(
        Executors.newFixedThreadPool(configuration.codecCallbackThreadPool.value.toInt()).asCoroutineDispatcher()
    )

    private val codecLatency = PipelineStatus.codecLatency

    private val responseObserver = object : StreamObserver<MessageGroupBatch> {
        override fun onNext(decodedBatch: MessageGroupBatch) {
            callbackScope.launch {

                val response = MessageGroupBatchWrapper(decodedBatch)

                LOGGER.trace { "codec response with hash ${response.responseHash} has been received" }

                pendingRequests.remove(response.requestId)?.let {
                    codecLatency.gaugeDec(listOf(it.streamName))
                    codecLatency.setDuration(it.startTimestamp.toDouble(), listOf(it.streamName))
                    it.completableDeferred.complete(response)
                }
                    ?: LOGGER.trace {
                        val firstSequence =
                            decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.sequence
                        val lastSequence =
                            decodedBatch.groupsList?.lastOrNull()?.messagesList?.lastOrNull()?.sequence
                        val stream =
                            "${decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.message?.sessionAlias}:${decodedBatch.groupsList.firstOrNull()?.messagesList?.firstOrNull()?.message?.direction.toString()}"
                        "codec response with hash ${response.responseHash} has no matching requests (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} requestId=${response.requestId})"
                    }
            }
        }

        override fun onError(t: Throwable?) = LOGGER.error(t) { "gRPC request failed." }
        override fun onCompleted() {}
    }

    override suspend fun sendToCodec(request: CodecBatchRequest): CodecBatchResponse {

        return withContext(requestSenderScope.coroutineContext) {
            while (pendingRequests.keys.size > maximumPendingRequests) {
                delay(100)
            }

            pendingRequests.computeIfAbsent(request.requestId) {
                val pendingRequest = request.toPending()

                callbackScope.launch {
                    delay(responseTimeout)

                    pendingRequest.completableDeferred.let {
                        if (it.isActive &&
                            pendingRequests[request.requestId]?.completableDeferred == pendingRequest.completableDeferred
                        ) {

                            pendingRequests.remove(request.requestId)
                            it.complete(null)

                            codecLatency.gaugeDec(listOf(request.streamName))
                            codecLatency.setDuration(
                                pendingRequest.startTimestamp.toDouble(),
                                listOf(request.streamName)
                            )

                            LOGGER.warn {
                                val firstSequence =
                                    request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sequence
                                val lastSequence =
                                    request.protobufRawMessageBatch.groupsList.last()?.messagesList?.last()?.rawMessage?.sequence
                                val stream =
                                    "${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sessionAlias}:${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.direction.toString()}"

                                "codec request timed out after $responseTimeout ms (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} hash=${request.requestHash}) requestId=${request.requestId}"
                            }
                        }
                    }
                }

                try {
                    if (usePinAttributes) {
                        val sessionAlias =
                            request.protobufRawMessageBatch.groupsList
                                .first().messagesList
                                .first().rawMessage.metadata.id.connectionId.sessionAlias

                        codecLatency.gaugeInc(listOf(request.streamName))
                        codecService.decode(request.protobufRawMessageBatch, mapOf("session-alias" to sessionAlias), responseObserver)
                    } else {
                        codecService.decode(request.protobufRawMessageBatch, emptyMap(), responseObserver)
                    }

                    LOGGER.trace {
                        val firstSequence =
                            request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sequence
                        val lastSequence =
                            request.protobufRawMessageBatch.groupsList.last()?.messagesList?.last()?.rawMessage?.sequence
                        val stream =
                            "${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.sessionAlias}:${request.protobufRawMessageBatch.groupsList.first()?.messagesList?.first()?.rawMessage?.direction.toString()}"

                        "codec request with hash ${request.requestHash} has been sent (stream=${stream} firstId=${firstSequence} lastId=${lastSequence} hash=${request.requestHash}) requestId=${request.requestId})"
                    }
                    LOGGER.debug { "codec request with hash ${request.requestHash.hashCode()} has been sent" }

                } catch (e: Exception) {
                    pendingRequest.completableDeferred.cancel(
                        "Unexpected exception while trying to send a codec request", e
                    )
                }
                pendingRequest
            }.toResponse()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}