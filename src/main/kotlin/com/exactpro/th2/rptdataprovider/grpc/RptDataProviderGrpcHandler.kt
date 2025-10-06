/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.rptdataprovider.grpc

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.EventData
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.grpc.FilterInfo
import com.exactpro.th2.dataprovider.grpc.FilterName
import com.exactpro.th2.dataprovider.grpc.IsMatched
import com.exactpro.th2.dataprovider.grpc.ListFilterName
import com.exactpro.th2.dataprovider.grpc.MatchRequest
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StringList
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.CodecResponseException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.GrpcWriter
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import com.exactpro.th2.rptdataprovider.handlers.events.SearchEventsCalledFun
import com.exactpro.th2.rptdataprovider.handlers.messages.SearchMessagesCalledFun
import com.exactpro.th2.rptdataprovider.metrics.measure
import com.exactpro.th2.rptdataprovider.metrics.withRequestId
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.TextFormat
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.ktor.utils.io.InternalAPI
import io.prometheus.client.Counter
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.coroutines.coroutineContext

//@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
class RptDataProviderGrpcHandler<B, G, RM, PM>(
    private val context: Context<B, G, RM, PM>,
    private val converterFun: (MessageWithMetadata<RM, PM>) -> List<MessageData>
) : DataProviderGrpc.DataProviderImplBase() {

    companion object {

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

        private val K_LOGGER = KotlinLogging.logger {}

        private fun makeRequestDescription(name: String, id: String, request: MessageOrBuilder): String {
            return "'$name ($id)' request with parameters '${TextFormat.shortDebugString(request)}'"
        }

    }

    val cradleService = this.context.cradleService
    private val eventCache = this.context.eventCache
    private val messageCache = this.context.messageCache
    private val checkRequestAliveDelay = context.configuration.checkRequestsAliveDelay.value.toLong()

    private val searchEventsHandler = this.context.searchEventsHandler
    private val searchMessagesHandler = this.context.searchMessagesHandler

    private val eventFiltersPredicateFactory = this.context.eventFiltersPredicateFactory
    private val messageFiltersPredicateFactory = this.context.messageFiltersPredicateFactory

    private val grpcThreadPoolSize = context.configuration.grpcThreadPoolSize.value.toInt()
    private val grpcThreadPool = Executors.newFixedThreadPool(grpcThreadPoolSize).asCoroutineDispatcher()



    private suspend fun checkContext(grpcContext: io.grpc.Context) {
        while (coroutineContext.isActive) {
            if (grpcContext.isCancelled)
                throw ChannelClosedException("Channel is closed")

            delay(checkRequestAliveDelay)
        }
    }

    private fun <T> sendErrorCode(responseObserver: StreamObserver<T>, e: Exception, status: Status) {
        responseObserver.onError(
            status.withDescription(ExceptionUtils.getRootCauseMessage(e) ?: e.toString()).asRuntimeException()
        )
    }

    private fun errorLogging(e: Exception, requestDescription: String, type: String) {
        K_LOGGER.error(e) { "unable to handle request $requestDescription - $type" }
    }

    private fun <T> handleRequest(
        responseObserver: StreamObserver<T>,
        useStream: Boolean,
        requestDescription: String,
        calledFun: suspend () -> Any
    ) {
        val context = io.grpc.Context.current()

        val handler = CoroutineExceptionHandler { _, exception ->
            K_LOGGER.error(exception) { "coroutine context exception from the handleRequest $requestDescription" }
        }

        CoroutineScope(grpcThreadPool + handler).launch {
            try {
                if (useStream) streamRequestGet.inc() else singleRequestGet.inc()
                try {
                    if (useStream) {
                        @Suppress("UNCHECKED_CAST")
                        handleSseRequest(
                            responseObserver as StreamObserver<StreamResponse>,
                            context,
                            calledFun.invoke() as suspend (StreamWriter<RM, PM>) -> Unit
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
            } catch (e: CancellationException) {
                K_LOGGER.debug(e) { "request processing was cancelled with CancellationException" }
            } catch (e: InvalidRequestException) {
                errorLogging(e, requestDescription, "invalid request")
                sendErrorCode(responseObserver, e, Status.INVALID_ARGUMENT)
            } catch (e: CradleObjectNotFoundException) {
                errorLogging(e, requestDescription, "missing cradle data")
                sendErrorCode(responseObserver, e, Status.NOT_FOUND)
            } catch (e: ChannelClosedException) {
                errorLogging(e, requestDescription, "channel closed")
                sendErrorCode(responseObserver, e, Status.DEADLINE_EXCEEDED)
            } catch (e: CodecResponseException) {
                errorLogging(e, requestDescription, "codec parses messages incorrectly")
                sendErrorCode(responseObserver, e, Status.INTERNAL)
            } catch (e: CradleIdException) {
                errorLogging(e, requestDescription, "unexpected cradle id exception")
                sendErrorCode(responseObserver, e, Status.INTERNAL)
            } catch (e: Exception) {
                errorLogging(e, requestDescription, "unexpected exception")
                sendErrorCode(responseObserver, e, Status.INTERNAL)
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
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }


    @ExperimentalCoroutinesApi
    @InternalAPI
    private suspend fun handleSseRequest(
        responseObserver: StreamObserver<StreamResponse>,
        grpcContext: io.grpc.Context,
        calledFun: suspend (StreamWriter<RM, PM>) -> Unit
    ) {
        coroutineScope {
            val job = launch {
                checkContext(grpcContext)
            }

            val grpcWriter = GrpcWriter(
                context.configuration.grpcWriterMessageBuffer.value.toInt(),
                responseObserver,
                this,
                converterFun
            )

            try {
                calledFun.invoke(grpcWriter)
            } finally {
                kotlin.runCatching {
                    grpcWriter.closeWriter()
                    job.cancel()
                }.onFailure { e ->
                    K_LOGGER.error(e) { "unexpected exception while closing grpc writer" }
                }
            }
        }
    }


    override fun getEvent(request: EventID, responseObserver: StreamObserver<EventData>) {
        withRequestId { requestId ->
            val requestName = "get event"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    eventCache.getOrPut(request.id)
                        .convertToEvent()
                        .convertToGrpcEventData()
                }
            }
        }
    }

    @InternalCoroutinesApi
    override fun getMessage(request: MessageID, responseObserver: StreamObserver<MessageData>) {
        withRequestId { requestId ->
            val requestName = "get message"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    val messageIdWithoutSubsequence = request.toBuilder().clearSubsequence().build()
                    messageCache.getOrPut(
                        StoredMessageId(
                            BookId(""),
                            messageIdWithoutSubsequence.connectionId.sessionAlias,
                            grpcDirectionToCradle(messageIdWithoutSubsequence.direction),
                            Instant.now(),
                            messageIdWithoutSubsequence.sequence
                        ).toString()
                    ).let {
                        converterFun(MessageWithMetadata(it, request))
                    }
                }
            }
        }
    }


    override fun getMessageStreams(request: com.google.protobuf.Empty, responseObserver: StreamObserver<StringList>) {
        withRequestId { requestId ->
            val requestName = "get message streams"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    val bookId = ""
                    StringList.newBuilder()
                        .addAllListString(cradleService.getSessionAliases(BookId(bookId)))
                        .build()
                }
            }
        }
    }


    @InternalCoroutinesApi
    @FlowPreview
    override fun searchMessages(grpcRequest: MessageSearchRequest, responseObserver: StreamObserver<StreamResponse>) {
        withRequestId { requestId ->
            val requestName = "grpc search message"
            val requestDescription = makeRequestDescription(requestName, requestId, grpcRequest)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = true, requestDescription = requestDescription) {
                    val filterPredicate = messageFiltersPredicateFactory.build(grpcRequest.filtersList)
                    val request: SseMessageSearchRequest<RM, PM> =
                        SseMessageSearchRequest(grpcRequest, filterPredicate, BookId(""))
                    SearchMessagesCalledFun(searchMessagesHandler, request)::calledFun
                }
            }
        }
    }

    @FlowPreview
    override fun searchEvents(grpcRequest: EventSearchRequest, responseObserver: StreamObserver<StreamResponse>) {
        withRequestId { requestId ->
            val requestName = "grpc search events"
            val requestDescription = makeRequestDescription(requestName, requestId, grpcRequest)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = true, requestDescription = requestDescription) {
                    val filterPredicate = eventFiltersPredicateFactory.build(grpcRequest.filtersList)
                    val request = SseEventSearchRequest(grpcRequest, filterPredicate, BookId(""))
                    SearchEventsCalledFun<RM, PM>(searchEventsHandler, request, requestId)::calledFun
                }
            }
        }
    }

    override fun getMessagesFilters(
        request: com.google.protobuf.Empty,
        responseObserver: StreamObserver<ListFilterName>
    ) {
        withRequestId { requestId ->
            val requestName = "get message filters names"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    ListFilterName.newBuilder()
                        .addAllFilterNames(
                            messageFiltersPredicateFactory.getFiltersNames().map {
                                FilterName.newBuilder().setFilterName(it).build()
                            }
                        ).build()
                }
            }
        }
    }


    override fun getEventsFilters(
        request: com.google.protobuf.Empty,
        responseObserver: StreamObserver<ListFilterName>
    ) {
        withRequestId { requestId ->
            val requestName = "get event filters names"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    ListFilterName.newBuilder()
                        .addAllFilterNames(
                            eventFiltersPredicateFactory.getFiltersNames().map {
                                FilterName.newBuilder().setFilterName(it).build()
                            }
                        ).build()
                }
            }
        }
    }


    override fun getEventFilterInfo(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        withRequestId { requestId ->
            val requestName = "get event filter info"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    eventFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
                }
            }
        }
    }


    override fun getMessageFilterInfo(request: FilterName, responseObserver: StreamObserver<FilterInfo>) {
        withRequestId { requestId ->
            val requestName = "get message filter info"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    messageFiltersPredicateFactory.getFilterInfo(request.filterName).convertToProto()
                }
            }
        }
    }


    override fun matchEvent(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
        withRequestId { requestId ->
            val requestName = "match event"
            val requestDescription = makeRequestDescription(requestName, requestId, request)
            measure(requestName, requestId, requestDescription, Level.INFO) {
                handleRequest(responseObserver, useStream = false, requestDescription = requestDescription) {
                    val filterPredicate = eventFiltersPredicateFactory.build(request.filtersList)
                    IsMatched.newBuilder().setIsMatched(
                        filterPredicate.apply(eventCache.getOrPut(request.eventId.id))
                    ).build()
                }
            }
        }
    }

    @InternalCoroutinesApi
    override fun matchMessage(request: MatchRequest, responseObserver: StreamObserver<IsMatched>) {
//        handleRequest(responseObserver, "match message", useStream = false, request = request) {
//            val filterPredicate = messageFiltersPredicateFactory.build(request.filtersList)
//            IsMatched.newBuilder().setIsMatched(
//                filterPredicate.apply(
//                    MessageWithMetadata(
//                        messageCache.getOrPut(
//                            StoredMessageId(
//                                request.messageId.connectionId.sessionAlias,
//                                grpcDirectionToCradle(request.messageId.direction),
//                                request.messageId.sequence
//                            ).toString()
//                        )
//                    )
//                )
//            ).build()
//        }
    }
}
