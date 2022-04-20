/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.grpc

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.*
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.CodecResponseException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.internal.FilteredMessageWrapper
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.EventGrpcWriter
import com.exactpro.th2.rptdataprovider.entities.sse.MessageGrpcWriter
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.TextFormat
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.ktor.server.engine.*
import io.ktor.util.*
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.concurrent.Executors
import kotlin.coroutines.coroutineContext
import kotlin.system.measureTimeMillis

private typealias Streaming = suspend (StreamWriter) -> Unit

@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
class RptDataProviderGrpcHandler(private val context: Context) : DataProviderGrpc.DataProviderImplBase() {

    data class StreamObserverWrapper<T>(val streamObserver: StreamObserver<T>, val isSseEvents: Boolean = false) 

    companion object {

        private val grpcStreamRequestsProcessedInParallelQuantity: Metrics =
            Metrics(
                "th2_grpc_stream_requests_processed_in_parallel_quantity",
                "GRPC stream requests processed in parallel"
            )

        private val grpcSingleRequestsProcessedInParallelQuantity: Metrics =
            Metrics(
                "th2_grpc_single_requests_processed_in_parallel_quantity",
                "GRPC single requests processed in parallel"
            )

        private val singleRequestGet: Counter =
            Counter.build("th2_grpc_single_requests_get", "GRPC single requests get")
                .register()

        private val singleRequestProcessed: Counter =
            Counter.build("th2_grpc_single_requests_processed", "GRPC single requests processed")
                .register()

        private val streamRequestGet: Counter =
            Counter.build("th2_grpc_stream_requests_get", "GRPC stream requests get")
                .register()

        private val streamRequestProcessed: Counter =
            Counter.build("th2_grpc_stream_requests_processed", "GRPC stream requests processed")
                .register()

        private val logger = KotlinLogging.logger {}

    }

    val cradleService = this.context.cradleService
    private val eventCache = this.context.eventCache
    private val messageCache = this.context.messageCache
    private val checkRequestAliveDelay = context.configuration.checkRequestsAliveDelay.value.toLong()

    private val searchEventsHandler = this.context.searchEventsHandler
    private val searchMessagesHandler = this.context.searchMessagesHandler

    private val eventFiltersPredicateFactory = this.context.eventFiltersPredicateFactory
    private val messageFiltersPredicateFactory = this.context.filteredMessageFiltersPredicateFactory

    private val grpcThreadPoolSize = context.configuration.grpcThreadPoolSize.value.toInt()
    private val grpcThreadPool = Executors.newFixedThreadPool(grpcThreadPoolSize).asCoroutineDispatcher()



    private suspend fun checkContext(grpcContext: io.grpc.Context) {
        while (coroutineContext.isActive) {
            if (grpcContext.isCancelled)
                throw ChannelClosedException("Channel is closed")

            delay(checkRequestAliveDelay)
        }
    }

    private fun <T> sendErrorCode(responseObserver: StreamObserverWrapper<T>, e: Exception, status: Status) {
        responseObserver.streamObserver.onError(
            status.withDescription(ExceptionUtils.getRootCauseMessage(e) ?: e.toString()).asRuntimeException()
        )
    }

    private fun errorLogging(e: Exception, requestName: String, stringParameters: String, type: String) {
        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - $type" }
    }

    private fun <T> handleRequest(
        responseObserver: StreamObserverWrapper<T>,
        requestName: String,
        useStream: Boolean,
        request: MessageOrBuilder?,
        calledFun: suspend () -> Any
    ) {
        val stringParameters = lazy { request?.let { TextFormat.shortDebugString(request) } ?: "" }
        val context = io.grpc.Context.current()

        val handler = CoroutineExceptionHandler { _, exception ->
            logger.error(exception) { "Coroutine context exception from the handleRequest method with $requestName" }
        }

        CoroutineScope(grpcThreadPool + handler).launch {
            logMetrics(if (useStream) grpcStreamRequestsProcessedInParallelQuantity else grpcSingleRequestsProcessedInParallelQuantity) {
                measureTimeMillis {
                    logger.debug { "handling '$requestName' request with parameters '${stringParameters.value}'" }
                    try {
                        if (useStream) streamRequestGet.inc() else singleRequestGet.inc()
                        try {
                            if (useStream) {
                                @Suppress("UNCHECKED_CAST")
                                handleSseRequest(
                                    responseObserver,
                                    context,
                                    calledFun.invoke() as Streaming
                                )
                            } else {
                                @Suppress("UNCHECKED_CAST")
                                handleRestApiRequest(responseObserver.streamObserver, context, calledFun as suspend () -> T)
                            }
                            responseObserver.streamObserver.onCompleted()
                        } catch (e: Exception) {
                            throw ExceptionUtils.getRootCause(e) ?: e
                        } finally {
                            if (useStream) streamRequestProcessed.inc() else singleRequestProcessed.inc()
                        }
                    } catch (e: CancellationException) {
                        logger.debug(e) { "request processing was cancelled with CancellationException" }
                    } catch (e: InvalidRequestException) {
                        errorLogging(e, requestName, stringParameters.value, "invalid request")
                        sendErrorCode(responseObserver, e, Status.INVALID_ARGUMENT)
                    } catch (e: CradleObjectNotFoundException) {
                        errorLogging(e, requestName, stringParameters.value, "missing cradle data")
                        sendErrorCode(responseObserver, e, Status.NOT_FOUND)
                    } catch (e: ChannelClosedException) {
                        errorLogging(e, requestName, stringParameters.value, "channel closed")
                        sendErrorCode(responseObserver, e, Status.DEADLINE_EXCEEDED)
                    } catch (e: CodecResponseException) {
                        errorLogging(e, requestName, stringParameters.value, "codec parses messages incorrectly")
                        sendErrorCode(responseObserver, e, Status.INTERNAL)
                    } catch (e: CradleIdException) {
                        errorLogging(e, requestName, stringParameters.value, "unexpected cradle id exception")
                        sendErrorCode(responseObserver, e, Status.INTERNAL)
                    } catch (e: Exception) {
                        errorLogging(e, requestName, stringParameters.value, "unexpected exception")
                        sendErrorCode(responseObserver, e, Status.INTERNAL)
                    }
                }.let {
                    logger.debug { "request '$requestName' with parameters '$stringParameters' handled - time=${it}ms" }
                }
            }
        }
    }

    private suspend fun <T> handleRestApiRequest(
        responseObserver: StreamObserver<T>,
        grpcContext: io.grpc.Context,
        calledFun: suspend () -> T
    ) {
        coroutineScope {
            try {
                launch {
                    checkContext(grpcContext)
                }
                responseObserver.onNext(calledFun.invoke())
                responseObserver.onCompleted()
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }


    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun <T> handleSseRequest(
        responseObserver: StreamObserverWrapper<T>,
        grpcContext: io.grpc.Context,
        calledFun: Streaming
    ) {
        coroutineScope {
            val job = launch {
                checkContext(grpcContext)
            }

            val grpcWriter = if (responseObserver.isSseEvents) {
                EventGrpcWriter(
                    context.configuration.grpcWriterMessageBuffer.value.toInt(),
                    responseObserver.streamObserver as StreamObserver<EventSearchResponse>,
                    context.jacksonMapper,
                    this
                )
            } else {
                MessageGrpcWriter(
                    context.configuration.grpcWriterMessageBuffer.value.toInt(),
                    responseObserver.streamObserver as StreamObserver<MessageSearchResponse>,
                    context.jacksonMapper,
                    this
                )
            }

            try {
                calledFun.invoke(grpcWriter)
            } finally {
                kotlin.runCatching {
                    grpcWriter.closeWriter()
                    job.cancel()
                }.onFailure { e ->
                    logger.error(e) { "unexpected exception while closing grpc writer" }
                }
            }
        }
    }


    override fun getEvent(request: EventID, responseObserver: StreamObserver<EventResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver), "get event", useStream = false, request = request) {
            eventCache.getOrPut(request.id)
                .convertToEvent()
                .convertToGrpcEventData()
        }
    }

    @InternalCoroutinesApi
    override fun getMessage(request: MessageID, responseObserver: StreamObserver<MessageGroupResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver), "get message", useStream = false, request = request) {
            val messageIdWithoutSubsequence = request.toBuilder().clearSubsequence().build()
            messageCache.getOrPut(
                StoredMessageId(
                    messageIdWithoutSubsequence.connectionId.sessionAlias,
                    grpcDirectionToCradle(messageIdWithoutSubsequence.direction),
                    messageIdWithoutSubsequence.sequence
                ).toString()
            ).let {
                MessageMapper.convertToGrpcMessageData(FilteredMessageWrapper(it, request.subsequenceList))
            }
        }
    }


    override fun getMessageStreams(
        request: MessageStreamsRequest,
        responseObserver: StreamObserver<MessageStreamsResponse>
    ) {
        handleRequest(StreamObserverWrapper(responseObserver), "get message streams", useStream = false, request = request) {
            MessageStreamsResponse.newBuilder()
                .addAllMessageStream(
                    cradleService.getMessageStreams().flatMap { stream ->
                        listOf(Direction.FIRST, Direction.SECOND).map {
                            MessageStream
                                .newBuilder()
                                .setDirection(it)
                                .setName(stream)
                                .build()
                        }
                    }
                )
                .build()
        }
    }


    @InternalCoroutinesApi
    @FlowPreview
    override fun searchMessages(
        grpcRequest: MessageSearchRequest,
        responseObserver: StreamObserver<MessageSearchResponse>
    ) {
        handleRequest(StreamObserverWrapper(responseObserver), "grpc search message", useStream = true, request = grpcRequest) {

            suspend fun(streamWriter: StreamWriter) {
                val filterPredicate = messageFiltersPredicateFactory.build(grpcRequest.filterList)
                val request = SseMessageSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest() // FIXME: Encapsulate into the SseMessageSearchRequest's constructor

                searchMessagesHandler.searchMessagesSse(request, streamWriter)
            }
        }
    }


    @FlowPreview
    override fun searchEvents(grpcRequest: EventSearchRequest, responseObserver: StreamObserver<EventSearchResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver, true), "grpc search events", useStream = true, request = grpcRequest) {

            suspend fun(streamWriter: StreamWriter) {
                val filterPredicate = eventFiltersPredicateFactory.build(grpcRequest.filterList)
                val request = SseEventSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest()

                searchEventsHandler.searchEventsSse(request, streamWriter)
            }
        }
    }


    override fun getMessagesFilters(
        request: MessageFiltersRequest,
        responseObserver: StreamObserver<FilterNamesResponse>
    ) {
        handleRequest(StreamObserverWrapper(responseObserver), "get message filters names", useStream = false, request = request) {
            FilterNamesResponse.newBuilder()
                .addAllFilterName(
                    messageFiltersPredicateFactory.getFiltersNames().map {
                        FilterName.newBuilder().setName(it).build()
                    }
                ).build()
        }
    }


    override fun getEventsFilters(
        request: EventFiltersRequest,
        responseObserver: StreamObserver<FilterNamesResponse>
    ) {
        handleRequest(StreamObserverWrapper(responseObserver), "get event filters names", useStream = false, request = request) {
            FilterNamesResponse.newBuilder()
                .addAllFilterName(
                    eventFiltersPredicateFactory.getFiltersNames().map {
                        FilterName.newBuilder().setName(it).build()
                    }
                ).build()
        }
    }


    override fun getEventFilterInfo(request: FilterInfoRequest, responseObserver: StreamObserver<FilterInfoResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver), "get event filter info", useStream = false, request = request) {
            eventFiltersPredicateFactory.getFilterInfo(request.filterName.name).convertToProto()
        }
    }


    override fun getMessageFilterInfo(
        request: FilterInfoRequest,
        responseObserver: StreamObserver<FilterInfoResponse>
    ) {
        handleRequest(StreamObserverWrapper(responseObserver), "get message filter info", useStream = false, request = request) {
            messageFiltersPredicateFactory.getFilterInfo(request.filterName.name).convertToProto()
        }
    }


    override fun matchEvent(request: EventMatchRequest, responseObserver: StreamObserver<MatchResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver), "match event", useStream = false, request = request) {
            val filterPredicate = eventFiltersPredicateFactory.build(request.filterList)
            MatchResponse.newBuilder().setMatch(
                filterPredicate.apply(eventCache.getOrPut(request.eventId.id))
            ).build()
        }
    }

    @InternalCoroutinesApi
    override fun matchMessage(request: MessageMatchRequest, responseObserver: StreamObserver<MatchResponse>) {
        handleRequest(StreamObserverWrapper(responseObserver), "match message", useStream = false, request = request) {
            val filterPredicate = messageFiltersPredicateFactory.build(request.filterList)
            MatchResponse.newBuilder().setMatch(
                filterPredicate.apply(
                    FilteredMessageWrapper(
                        messageCache.getOrPut(
                            StoredMessageId(
                                request.messageId.connectionId.sessionAlias,
                                grpcDirectionToCradle(request.messageId.direction),
                                request.messageId.sequence
                            ).toString()
                        )
                    )
                )
            ).build()
        }
    }
}
