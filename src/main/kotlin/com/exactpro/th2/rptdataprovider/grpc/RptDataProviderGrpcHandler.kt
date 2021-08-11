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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.*
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.Metrics
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.GrpcWriter
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import com.exactpro.th2.rptdataprovider.logMetrics
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.TextFormat
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.ktor.server.engine.EngineAPI
import io.ktor.util.InternalAPI
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.system.measureTimeMillis


private typealias Streaming =
        suspend (StreamWriter, suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit) -> Unit

@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
class RptDataProviderGrpcHandler(private val context: Context) : DataProviderGrpc.DataProviderImplBase() {

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
    private val jacksonMapper = context.jacksonMapper
    private val checkRequestAliveDelay = context.configuration.checkRequestsAliveDelay.value.toLong()
    private val keepAliveTimeout = context.configuration.keepAliveTimeout.value.toLong()
    private val getEventsLimit = this.context.configuration.eventSearchChunkSize.value.toInt()

    private val searchEventsHandler = this.context.searchEventsHandler
    private val searchMessagesHandler = this.context.searchMessagesHandler

    private val eventFiltersPredicateFactory = this.context.eventFiltersPredicateFactory
    private val messageFiltersPredicateFactory = this.context.messageFiltersPredicateFactory

    private val sseEventSearchStep = this.context.sseEventSearchStep


    private suspend fun checkContext(context: io.grpc.Context) {
        while (coroutineContext.isActive) {
            if (context.isCancelled)
                throw ChannelClosedException("Channel is closed")

            delay(checkRequestAliveDelay)
        }
    }


    private suspend fun keepAlive(
        writer: StreamWriter,
        lastScannedObjectInfo: LastScannedObjectInfo,
        counter: AtomicLong
    ) {
        while (coroutineContext.isActive) {
            writer.write(lastScannedObjectInfo, counter)
            delay(keepAliveTimeout)
        }
    }

    private fun <T> sendErrorCode(responseObserver: StreamObserver<T>, e: Exception, status: Status) {
        responseObserver.onError(
            status.withDescription(ExceptionUtils.getRootCauseMessage(e) ?: e.toString()).asRuntimeException()
        )
    }

    private fun errorLogging(e: Exception, requestName: String, stringParameters: String, type: String) {
        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - $type" }
    }

    private fun <T> handleRequest(
        responseObserver: StreamObserver<T>,
        requestName: String,
        useStream: Boolean,
        request: MessageOrBuilder?,
        calledFun: suspend () -> Any
    ) {
        val stringParameters = lazy { request?.let { TextFormat.shortDebugString(request) } ?: "" }
        val context = io.grpc.Context.current()

        CoroutineScope(Dispatchers.Default).launch {
            logMetrics(if (useStream) grpcStreamRequestsProcessedInParallelQuantity else grpcSingleRequestsProcessedInParallelQuantity) {
                measureTimeMillis {
                    logger.debug { "handling '$requestName' request with parameters '${stringParameters.value}'" }
                    try {
                        if (useStream) streamRequestGet.inc() else singleRequestGet.inc()
                        try {
                            if (useStream) {
                                @Suppress("UNCHECKED_CAST")
                                handleSseRequest(
                                    responseObserver as StreamObserver<StreamResponse>,
                                    context,
                                    calledFun.invoke() as Streaming
                                )
                            } else {
                                handleRestApiRequest(responseObserver, context, calledFun as suspend () -> T)
                            }

                            responseObserver.onCompleted()
                        } catch (e: Exception) {
                            throw ExceptionUtils.getRootCause(e) ?: e
                        } finally {
                            if (useStream) streamRequestProcessed.inc() else singleRequestProcessed.inc()
                        }
                    } catch (e: InvalidRequestException) {
                        errorLogging(e, requestName, stringParameters.value, "invalid request")
                        sendErrorCode(responseObserver, e, Status.INVALID_ARGUMENT)
                    } catch (e: CradleObjectNotFoundException) {
                        errorLogging(e, requestName, stringParameters.value, "missing cradle data")
                        sendErrorCode(responseObserver, e, Status.NOT_FOUND)
                    } catch (e: ChannelClosedException) {
                        errorLogging(e, requestName, stringParameters.value, "channel closed")
                        sendErrorCode(responseObserver, e, Status.DEADLINE_EXCEEDED)
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
        context: io.grpc.Context,
        calledFun: suspend () -> T
    ) {
        coroutineScope {
            try {
                launch {
                    checkContext(context)
                }
                responseObserver.onNext(calledFun.invoke())
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }


    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun handleSseRequest(
        responseObserver: StreamObserver<StreamResponse>,
        context: io.grpc.Context,
        calledFun: Streaming
    ) {
        coroutineScope {
            try {
                launch {
                    checkContext(context)
                }
                calledFun.invoke(GrpcWriter(responseObserver), ::keepAlive)
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }


    override fun getEvent(request: EventID, responseObserver: StreamObserver<EventData>) {
        handleRequest(responseObserver, "get event", useStream = false, request = request) {
            eventCache.getOrPut(request.id)
                .convertToEvent()
                .convertToGrpcEventData()
        }
    }

    override fun getEvents(request: EventIds, responseObserver: StreamObserver<Events>) {
        handleRequest(responseObserver, "get events", useStream = false, request = request) {
            val ids = request.idsList.map { it.id }
            when {
                ids.isNullOrEmpty() ->
                    throw InvalidRequestException("Ids set must not be empty: $ids")
                ids.size > getEventsLimit ->
                    throw InvalidRequestException("Too many id in request: ${ids.size}, max is: $getEventsLimit")
                else -> eventCache.getOrPutMany(ids.toSet())
                    .map { it.convertToEvent().convertToGrpcEventData() }
            }.let {
                Events.newBuilder().addAllEvents(it).build()
            }
        }
    }

    override fun getMessage(request: MessageID, responseObserver: StreamObserver<MessageData>) {
        handleRequest(responseObserver, "get message", useStream = false, request = request) {
            val messageIdWithoutSubsequence = request.toBuilder().clearSubsequence().build()
            messageCache.getOrPut(
                StoredMessageId(
                    messageIdWithoutSubsequence.connectionId.sessionAlias,
                    grpcDirectionToCradle(messageIdWithoutSubsequence.direction),
                    messageIdWithoutSubsequence.sequence
                ).toString()
            ).let {
                MessageMapper.convertToGrpcMessageData(MessageWithMetadata(it, request))
            }
        }
    }


    override fun getMessageStreams(request: com.google.protobuf.Empty, responseObserver: StreamObserver<StringList>) {
        handleRequest(
            responseObserver,
            "get message streams",
            useStream = false,
            request = request
        ) {
            StringList.newBuilder()
                .addAllListString(cradleService.getMessageStreams())
                .build()
        }
    }

    @FlowPreview
    override fun searchMessages(
        grpcRequest: MessageSearchRequest,
        responseObserver: StreamObserver<StreamResponse>
    ) {
        handleRequest(
            responseObserver,
            "grpc search message",
            useStream = true,
            request = grpcRequest
        ) {
            suspend fun(
                w: StreamWriter,
                keepAlive: suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit
            ) {

                val filterPredicate = messageFiltersPredicateFactory.build(grpcRequest.filtersList)
                val request = SseMessageSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest()

                searchMessagesHandler.searchMessagesSse(request, jacksonMapper, keepAlive, w)
            }
        }
    }

    @FlowPreview
    override fun searchEvents(
        grpcRequest: EventSearchRequest,
        responseObserver: StreamObserver<StreamResponse>
    ) {
        handleRequest(
            responseObserver,
            "grpc search events",
            useStream = true,
            request = grpcRequest
        ) {
            suspend fun(
                w: StreamWriter,
                keepAlive: suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit
            ) {
                val filterPredicate = eventFiltersPredicateFactory.build(grpcRequest.filtersList)
                val request = SseEventSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest()

                searchEventsHandler.searchEventsSse(request, jacksonMapper, sseEventSearchStep, keepAlive, w)
            }
        }
    }

    override fun getMessagesFilters(
        request: com.google.protobuf.Empty,
        responseObserver: StreamObserver<ListFilterName>
    ) {
        handleRequest(
            responseObserver,
            "get message filters names",
            useStream = false,
            request = request
        ) {
            ListFilterName.newBuilder()
                .addAllFilterNames(
                    messageFiltersPredicateFactory.getFiltersNames().map {
                        FilterName.newBuilder().setFilterName(it).build()
                    }
                ).build()
        }
    }


    override fun getEventsFilters(
        request: com.google.protobuf.Empty,
        responseObserver: StreamObserver<ListFilterName>
    ) {
        handleRequest(
            responseObserver,
            "get event filters names",
            useStream = false,
            request = request
        ) {
            ListFilterName.newBuilder()
                .addAllFilterNames(
                    eventFiltersPredicateFactory.getFiltersNames().map {
                        FilterName.newBuilder().setFilterName(it).build()
                    }
                ).build()
        }
    }

    override fun getEventFilterInfo(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        handleRequest(responseObserver, "get event filter info", useStream = false, request = request) {
            eventFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
        }
    }

    override fun getMessageFilterInfo(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        handleRequest(
            responseObserver,
            "get message filter info",
            useStream = false,
            request = request
        ) {
            messageFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
        }
    }

    override fun matchEvent(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
        handleRequest(responseObserver, "match event", useStream = false, request = request) {
            val filterPredicate = eventFiltersPredicateFactory.build(request.filtersList)
            IsMatched.newBuilder().setIsMatched(
                filterPredicate.apply(eventCache.getOrPut(request.eventId.id))
            ).build()
        }
    }

    override fun matchMessage(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
        handleRequest(responseObserver, "match message", useStream = false, request = request) {
            val filterPredicate = messageFiltersPredicateFactory.build(request.filtersList)
            IsMatched.newBuilder().setIsMatched(
                filterPredicate.apply(
                    MessageWithMetadata(
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