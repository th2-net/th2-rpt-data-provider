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

package com.exactpro.th2.rptdataprovider.server

import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.*
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import io.ktor.application.*
import io.ktor.features.Compression
import io.ktor.http.CacheControl
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.request.uri
import io.ktor.response.cacheControl
import io.ktor.response.respondText
import io.ktor.response.respondTextWriter
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.EngineAPI
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.util.AttributeKey
import io.ktor.util.InternalAPI
import io.ktor.util.rootCause
import io.ktor.util.toMap
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.system.measureTimeMillis

private typealias Streaming = suspend (StreamWriter) -> Unit


class HttpServer(private val context: Context) {

    private val sseRequestsProcessedInParallelQuantity: Metrics =
        Metrics("th2_sse_requests_processed_in_parallel_quantity", "SSE requests processed in parallel")

    private val restRequestsProcessedInParallelQuantity: Metrics =
        Metrics("th2_rest_requests_processed_in_parallel_quantity", "REST requests processed in parallel")

    private val restRequestGet: Counter =
        Counter.build("th2_rest_requests_get", "REST requests get")
            .register()

    private val restRequestProcessed: Counter =
        Counter.build("th2_rest_requests_processed", "REST requests processed")
            .register()


    private val sseRequestGet: Counter =
        Counter.build("th2_sse_requests_get", "SSE requests get")
            .register()

    private val sseRequestProcessed: Counter =
        Counter.build("th2_sse_requests_processed", "SSE requests processed")
            .register()


    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val jacksonMapper = context.jacksonMapper
    private val checkRequestAliveDelay = context.configuration.checkRequestsAliveDelay.value.toLong()
    private val keepAliveTimeout = context.configuration.keepAliveTimeout.value.toLong()
    private val configuration = context.configuration

    private class Timeouts {
        class Config(var requestTimeout: Long = 5000L, var excludes: List<String> = listOf("sse"))

        companion object : ApplicationFeature<ApplicationCallPipeline, Config, Unit> {
            override val key: AttributeKey<Unit> = AttributeKey("Timeouts")

            override fun install(pipeline: ApplicationCallPipeline, configure: Config.() -> Unit) {
                val config = Config().apply(configure)
                val timeout = config.requestTimeout
                val excludes = config.excludes

                if (timeout <= 0) return

                pipeline.intercept(ApplicationCallPipeline.Features) {
                    if (excludes.any { call.request.uri.contains(it) }) return@intercept
                    withTimeout(timeout) {
                        proceed()
                    }
                }
            }
        }
    }

    @EngineAPI
    @InternalAPI
    suspend fun checkContext(context: ApplicationCall) {
        context.javaClass.getDeclaredField("call").also {
            it.trySetAccessible()
            val nettyApplicationRequest = it.get(context) as NettyApplicationCall

            while (coroutineContext.isActive) {
                if (nettyApplicationRequest.context.isRemoved)
                    throw ClosedChannelException()

                delay(checkRequestAliveDelay)
            }
        }
    }


    @InternalAPI
    private suspend fun sendErrorCode(call: ApplicationCall, e: Exception, code: HttpStatusCode) {
        withContext(NonCancellable) {
            call.respondText(e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, code)
        }
    }

    @InternalAPI
    private suspend fun sendErrorCodeOrEmptyJson(
        probe: Boolean,
        call: ApplicationCall,
        e: Exception,
        code: HttpStatusCode
    ) {
        withContext(NonCancellable) {
            if (probe) {
                call.respondText(
                    jacksonMapper.writeValueAsString(null), ContentType.Application.Json
                )
            } else {
                sendErrorCode(call, e, code)
            }
        }
    }

    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun handleRequest(
        call: ApplicationCall,
        context: ApplicationCall,
        requestName: String,
        cacheControl: CacheControl?,
        probe: Boolean,
        useSse: Boolean,
        vararg parameters: Any?,
        calledFun: suspend () -> Any
    ) {
        val stringParameters = lazy { parameters.contentDeepToString() }
        logMetrics(if (useSse) sseRequestsProcessedInParallelQuantity else restRequestsProcessedInParallelQuantity) {
            coroutineScope {
                measureTimeMillis {
                    logger.debug { "handling '$requestName' request with parameters '${stringParameters.value}'" }
                    try {
                        if (useSse) sseRequestGet.inc() else restRequestGet.inc()
                        try {
                            if (useSse) {
                                val function = calledFun.invoke()
                                @Suppress("UNCHECKED_CAST")
                                handleSseRequest(call, context, function as Streaming)
                            } else {
                                handleRestApiRequest(call, context, cacheControl, probe, calledFun)
                            }
                        } catch (e: Exception) {
                            throw e.rootCause ?: e
                        } finally {
                            if (useSse) sseRequestProcessed.inc() else restRequestProcessed.inc()
                        }
                    } catch (e: InvalidRequestException) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '${stringParameters.value}' - invalid request" }
                    } catch (e: CradleObjectNotFoundException) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '${stringParameters.value}' - missing cradle data" }
                    } catch (e: ClosedChannelException) {
                        logger.info { "request '$requestName' with parameters '${stringParameters.value}' has been cancelled by a client" }
                    } catch (e: Exception) {
                        logger.error(e) { "unable to handle request '$requestName' with parameters '${stringParameters.value}' - unexpected exception" }
                    }
                }.let { logger.debug { "request '$requestName' with parameters '${stringParameters.value}' handled - time=${it}ms" } }
            }
        }
    }


    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun handleSseRequest(
        call: ApplicationCall,
        context: ApplicationCall,
        calledFun: Streaming
    ) {
        coroutineScope {
            launch {
                launch {
                    checkContext(context)
                }
                call.response.headers.append(HttpHeaders.CacheControl, "no-cache, no-store")
                call.respondTextWriter(contentType = ContentType.Text.EventStream) {
                    try {
                        calledFun.invoke(SseWriter(this, jacksonMapper))
                    } catch (e: Exception) {
                        eventWrite(SseEvent.build(jacksonMapper, e))
                        throw e
                    } finally {
                        eventWrite(SseEvent(event = EventType.CLOSE))
                        closeWriter()
                    }
                }
                coroutineContext.cancelChildren()
            }.join()
        }
    }

    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    private suspend fun handleRestApiRequest(
        call: ApplicationCall,
        context: ApplicationCall,
        cacheControl: CacheControl?,
        probe: Boolean,
        calledFun: suspend () -> Any
    ) {
        coroutineScope {
            try {
                launch {
                    launch {
                        checkContext(context)
                    }
                    cacheControl?.let { call.response.cacheControl(it) }
                    call.respondText(
                        jacksonMapper.asStringSuspend(calledFun.invoke()),
                        ContentType.Application.Json
                    )
                    coroutineContext.cancelChildren()
                }.join()
            } catch (e: Exception) {
                when (val exception = e.rootCause ?: e) {
                    is InvalidRequestException -> sendErrorCode(call, exception, HttpStatusCode.BadRequest)
                    is CradleObjectNotFoundException -> sendErrorCodeOrEmptyJson(
                        probe, call, exception, HttpStatusCode.NotFound
                    )
                    is ChannelClosedException -> sendErrorCode(call, exception, HttpStatusCode.RequestTimeout)
                    is CradleIdException -> sendErrorCodeOrEmptyJson(probe, call, e, HttpStatusCode.InternalServerError)
                    else -> sendErrorCode(call, exception as Exception, HttpStatusCode.InternalServerError)
                }
                throw e
            }
        }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    fun run() {

        val notModifiedCacheControl = this.context.cacheControlNotModified
        val rarelyModifiedCacheControl = this.context.cacheControlRarelyModified

        val cradleService = this.context.cradleService

        val eventCache = this.context.eventCache
        val messageCache = this.context.messageCache

        val searchEventsHandler = this.context.searchEventsHandler
        val searchMessagesHandler = this.context.searchMessagesHandler

        val eventFiltersPredicateFactory = this.context.eventFiltersPredicateFactory
        val messageFiltersPredicateFactory = this.context.messageFiltersPredicateFactory

        val getEventsLimit = this.context.configuration.eventSearchChunkSize.value.toInt()

        embeddedServer(Netty, configuration.port.value.toInt()) {

            install(Compression)
            install(Timeouts) {
                requestTimeout = context.timeout
            }

            routing {

                get("/event/{id}") {
                    val probe = call.parameters["probe"]?.toBoolean() ?: false
                    handleRequest(
                        call, context, "get single event", notModifiedCacheControl, probe,
                        false, call.parameters.toMap()
                    ) {
                        eventCache.getOrPut(call.parameters["id"]!!).convertToEvent()
                    }
                }

                get("/events") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(
                        call, context, "get single event", notModifiedCacheControl, false,
                        false, queryParametersMap
                    ) {
                        val ids = queryParametersMap["ids"]
                        when {
                            ids.isNullOrEmpty() ->
                                throw InvalidRequestException("Ids set must not be empty: $ids")
                            ids.size > getEventsLimit ->
                                throw InvalidRequestException("Too many id in request: ${ids.size}, max is: $getEventsLimit")
                            else -> eventCache.getOrPutMany(ids.toSet()).map { it.convertToEvent() }
                        }
                    }
                }

                get("/messageStreams") {
                    handleRequest(
                        call, context, "get message streams", rarelyModifiedCacheControl,
                        probe = false, useSse = false
                    ) {
                        cradleService.getMessageStreams()
                    }
                }

                get("/message/{id}") {
                    val probe = call.parameters["probe"]?.toBoolean() ?: false
                    handleRequest(
                        call, context, "get single message",
                        notModifiedCacheControl, probe, false, call.parameters.toMap()
                    ) {
                        MessageWithMetadata(messageCache.getOrPut(call.parameters["id"]!!)).let {
                            MessageMapper.convertToHttpMessage(it)
                        }
                    }
                }

                get("search/sse/messages") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "search messages sse", null, false, true, queryParametersMap) {
                        suspend fun(streamWriter: StreamWriter) {
                            val filterPredicate = messageFiltersPredicateFactory.build(queryParametersMap)
                            val request = SseMessageSearchRequest(queryParametersMap, filterPredicate)
                            request.checkRequest()
                            searchMessagesHandler.searchMessagesSse(request, streamWriter)
                        }
                    }
                }

                get("search/sse/events") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "search events sse", null, false, true, queryParametersMap) {
                        suspend fun(streamWriter: StreamWriter) {
                            val filterPredicate = eventFiltersPredicateFactory.build(queryParametersMap)
                            val request = SseEventSearchRequest(queryParametersMap, filterPredicate)
                            request.checkRequest()

                            searchEventsHandler.searchEventsSse(request, streamWriter)
                        }
                    }
                }

                get("filters/sse-messages") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "get message filters", null, false, false, queryParametersMap) {
                        messageFiltersPredicateFactory.getFiltersNames()
                    }
                }

                get("filters/sse-events") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "get event filters", null, false, false, queryParametersMap) {
                        eventFiltersPredicateFactory.getFiltersNames()
                    }
                }

                get("filters/sse-messages/{name}") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "get message filters", null, false, false, queryParametersMap) {
                        messageFiltersPredicateFactory.getFilterInfo(call.parameters["name"]!!)
                    }
                }


                get("filters/sse-events/{name}") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "get event filters", null, false, false, queryParametersMap) {
                        eventFiltersPredicateFactory.getFilterInfo(call.parameters["name"]!!)
                    }
                }

                get("match/event/{id}") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "match event", null, false, false, queryParametersMap) {
                        val filterPredicate = eventFiltersPredicateFactory.build(queryParametersMap)
                        filterPredicate.apply(eventCache.getOrPut(call.parameters["id"]!!))
                    }
                }

                get("/match/message/{id}") {
                    val queryParametersMap = call.request.queryParameters.toMap()
                    handleRequest(call, context, "match message", null, false, false, queryParametersMap) {
                        val filterPredicate = messageFiltersPredicateFactory.build(queryParametersMap)
                        val message = messageCache.getOrPut(call.parameters["id"]!!)
                        filterPredicate.apply(MessageWithMetadata(message))
                    }
                }
            }
        }.start(false)

        logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }
    }
}