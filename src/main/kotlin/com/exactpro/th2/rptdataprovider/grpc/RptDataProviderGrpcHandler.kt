/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.Direction.SECOND
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.GrpcWriter
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.ktor.server.engine.*
import io.ktor.util.*
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
class RptDataProviderGrpcHandler(private val context: Context) : RptDataProviderGrpc.RptDataProviderImplBase() {

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
    private val configuration = context.configuration

    val notModifiedCacheControl = this.context.cacheControlNotModified
    val rarelyModifiedCacheControl = this.context.cacheControlRarelyModified

    val searchEventsHandler = this.context.searchEventsHandler
    val searchMessagesHandler = this.context.searchMessagesHandler

    private val eventFiltersPredicateFactory = this.context.eventFiltersPredicateFactory
    private val messageFiltersPredicateFactory = this.context.messageFiltersPredicateFactory

    val sseEventSearchStep = this.context.sseEventSearchStep


    private suspend fun checkContext() {
        while (coroutineContext.isActive) {
            if (io.grpc.Context.current().isCancelled)
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

    private fun <T> sendErrorCodeOrEmptyJson(
        probe: Boolean, responseObserver: StreamObserver<T>, e: Exception, status: Status
    ) {
        if (probe) {
//            responseObserver.onNext(Empty.getDefaultInstance())
//            responseObserver.onCompleted()
        } else {
            sendErrorCode(responseObserver, e, status)
        }
    }

    private fun <T> handleRequest(
        responseObserver: StreamObserver<T>,
        requestName: String,
        probe: Boolean,
        useStream: Boolean,
        request: Any?,
        calledFun: () -> Any
    ) {
        val stringParameters = request.toString()
        CoroutineScope(Dispatchers.Default).launch {
            logMetrics(if (useStream) grpcStreamRequestsProcessedInParallelQuantity else grpcSingleRequestsProcessedInParallelQuantity) {
                measureTimeMillis {
                    logger.debug { "handling '$requestName' request with parameters '$stringParameters'" }
                    try {
                        if (useStream) streamRequestGet.inc() else singleRequestGet.inc()
                        try {
                            val function = calledFun.invoke()
                            if (useStream) {
                                @Suppress("UNCHECKED_CAST")
                                handleSseRequest(
                                    responseObserver as StreamObserver<StreamResponse>,
                                    function as Streaming
                                )
                            } else {
                                handleRestApiRequest(responseObserver, probe, function as suspend () -> T)
                            }
                        } catch (e: Exception) {
                            throw ExceptionUtils.getRootCause(e) ?: e
                        } finally {
                            if (useStream) streamRequestProcessed.inc() else singleRequestProcessed.inc()
                        }
                    } catch (e: InvalidRequestException) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - invalid request" }
                    } catch (e: CradleObjectNotFoundException) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - missing cradle data" }
                    } catch (e: ChannelClosedException) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - channel closed" }
                    } catch (e: Exception) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - unexpected exception" }
                    }
                }.let { logger.debug { "request '$requestName' with parameters '$stringParameters' handled - time=${it}ms" } }
            }
        }
    }

    private suspend fun <T> handleRestApiRequest(
        responseObserver: StreamObserver<T>,
        probe: Boolean,
        calledFun: suspend () -> T
    ) {
        coroutineScope {
            try {
                launch {
                    launch {
                        checkContext()
                    }
                    responseObserver.onNext(calledFun.invoke())
                    responseObserver.onCompleted()
                    coroutineContext.cancelChildren()
                }.join()
            } catch (e: Exception) {
                when (val exception = ExceptionUtils.getRootCause(e) ?: e) {
                    is InvalidRequestException -> sendErrorCode(responseObserver, exception, Status.INVALID_ARGUMENT)
                    is CradleObjectNotFoundException -> sendErrorCodeOrEmptyJson(
                        probe, responseObserver, exception, Status.NOT_FOUND
                    )
                    is ChannelClosedException -> sendErrorCode(responseObserver, exception, Status.DEADLINE_EXCEEDED)
                    is CradleIdException -> sendErrorCodeOrEmptyJson(probe, responseObserver, e, Status.INTERNAL)
                    else -> sendErrorCode(responseObserver, exception as Exception, Status.INTERNAL)
                }
                throw e
            }
        }
    }


    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun handleSseRequest(
        responseObserver: StreamObserver<StreamResponse>,
        calledFun: suspend (StreamWriter, suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit) -> Unit
    ) {
        coroutineScope {
            launch {
                try {
                    launch {
                        checkContext()
                    }
                    calledFun.invoke(GrpcWriter(responseObserver), ::keepAlive)
                } catch (e: Exception) {
                    val rootCause = ExceptionUtils.getRootCause(e)
                    responseObserver.onError(rootCause)
                } finally {
                    responseObserver.onCompleted()
                }
                coroutineContext.cancelChildren()
            }.join()
        }
    }


    override fun getEvent(request: EventID, responseObserver: StreamObserver<RptEvent>) {
        handleRequest(responseObserver, "get event", probe = true, useStream = false, request = request) {
            suspend fun(): RptEvent {
                return eventCache.getOrPut(request.id)
                    .convertToEvent()
                    .convertToGrpcRptEvent()
            }
        }
    }


    override fun getMessage(request: MessageID, responseObserver: StreamObserver<RptMessage>) {
        handleRequest(responseObserver, "get message", probe = true, useStream = false, request = request) {
            suspend fun(): RptMessage {
                return messageCache.getOrPut(
                    StoredMessageId(
                        request.connectionId.sessionAlias,
                        if (request.direction == Direction.FIRST) FIRST else SECOND,
                        request.sequence
                    ).toString()
                ).convertToGrpcRptMessage()
            }
        }
    }


    override fun getMessageStreams(request: Empty, responseObserver: StreamObserver<StringList>) {
        handleRequest(responseObserver, "get message streams", probe = false, useStream = false, request = request) {
            suspend fun(): StringList {
                return StringList.newBuilder()
                    .addAllListString(cradleService.getMessageStreams())
                    .build()
            }
        }
    }

    @FlowPreview
    override fun searchSseMessages(
        grpcRequest: GrpcMessageSearchRequest,
        responseObserver: StreamObserver<StreamResponse>
    ) {
        handleRequest(responseObserver, "grpc search message", probe = false, useStream = true, request = grpcRequest) {
            suspend fun(w: StreamWriter, keepAlive: suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit) {

                val filterPredicate = messageFiltersPredicateFactory.build(grpcRequest.filtersList)
                val request = SseMessageSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest()

                searchMessagesHandler.searchMessagesSse(request, jacksonMapper, keepAlive, w)
            }
        }
    }

    @FlowPreview
    override fun searchSseEvents(
        grpcRequest: GrpcEventSearchRequest,
        responseObserver: StreamObserver<StreamResponse>
    ) {
        handleRequest(responseObserver, "grpc search events", probe = false, useStream = true, request = grpcRequest) {
            suspend fun(w: StreamWriter, keepAlive: suspend (StreamWriter, LastScannedObjectInfo, AtomicLong) -> Unit) {

                val filterPredicate = eventFiltersPredicateFactory.build(grpcRequest.filtersList)
                val request = SseEventSearchRequest(grpcRequest, filterPredicate)
                request.checkRequest()

                searchEventsHandler.searchEventsSse(request, jacksonMapper, sseEventSearchStep, keepAlive, w)
            }
        }
    }

    override fun filtersSseMessages(request: Empty, responseObserver: StreamObserver<ListFilterName>) {
        handleRequest(responseObserver,
            "get message filters names",
            probe = false,
            useStream = false,
            request = request) {
            suspend fun(): ListFilterName {
                return ListFilterName.newBuilder()
                    .addAllFilterNames(
                        messageFiltersPredicateFactory.getFiltersNames().map {
                            FilterName.newBuilder().setFilterName(it).build()
                        }
                    ).build()
            }
        }
    }

    override fun filtersSseEvents(request: Empty, responseObserver: StreamObserver<ListFilterName>) {
        handleRequest(responseObserver,
            "get event filters names",
            probe = false,
            useStream = false,
            request = request) {
            suspend fun(): ListFilterName {
                return ListFilterName.newBuilder()
                    .addAllFilterNames(
                        eventFiltersPredicateFactory.getFiltersNames().map {
                            FilterName.newBuilder().setFilterName(it).build()
                        }
                    ).build()
            }
        }
    }

    override fun filterSseEvents(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        handleRequest(responseObserver, "get event filters", probe = false, useStream = false, request = request) {
            suspend fun(): FilterInfo {
                return eventFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
            }
        }
    }

    override fun filterSseMessages(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        handleRequest(responseObserver, "get message filters", probe = false, useStream = false, request = request) {
            suspend fun(): FilterInfo {
                return messageFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
            }
        }
    }

    override fun matchEvent(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
        handleRequest(responseObserver, "match event", probe = false, useStream = false, request = request) {
            suspend fun(): IsMatched {
                val filterPredicate = eventFiltersPredicateFactory.build(request.filtersList)
                return IsMatched.newBuilder().setIsMatched(
                    filterPredicate.apply(eventCache.getOrPut(request.eventId.id))
                ).build()
            }
        }
    }

    override fun matchMessage(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
        handleRequest(responseObserver, "match message", probe = false, useStream = false, request = request) {
            suspend fun(): IsMatched {
                val filterPredicate = messageFiltersPredicateFactory.build(request.filtersList)
                return IsMatched.newBuilder().setIsMatched(
                    filterPredicate.apply(messageCache.getOrPut(
                        StoredMessageId(
                            request.messageId.connectionId.sessionAlias,
                            if (request.messageId.direction == Direction.FIRST) FIRST else SECOND,
                            request.messageId.sequence
                        ).toString()
                    ))
                ).build()
            }
        }
    }
}