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

package com.exactpro.th2.rptdataprovider.server

import com.exactpro.cradle.BookId
import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.CodecResponseException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.exactpro.th2.rptdataprovider.entities.sse.EventType
import com.exactpro.th2.rptdataprovider.entities.sse.HttpWriter
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.events.SearchEventsCalledFun
import com.exactpro.th2.rptdataprovider.handlers.messages.SearchMessagesCalledFun
import com.exactpro.th2.rptdataprovider.metrics.measure
import com.exactpro.th2.rptdataprovider.metrics.withRequestId
import com.exactpro.th2.rptdataprovider.server.handler.AbortableRequestHandler
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.ktor.http.CacheControl
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.BaseApplicationPlugin
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.compression.Compression
import io.ktor.server.request.uri
import io.ktor.server.response.cacheControl
import io.ktor.server.response.respondText
import io.ktor.server.response.respondTextWriter
import io.ktor.server.routing.IgnoreTrailingSlash
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.util.getOrFail
import io.ktor.util.AttributeKey
import io.ktor.util.rootCause
import io.ktor.util.toMap
import io.ktor.utils.io.InternalAPI
import io.prometheus.client.Counter
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext

class HttpServer<B, G, RM, PM>(
    private val applicationContext: Context<B, G, RM, PM>,
    private val convertFun: (messageWithMetadata: MessageWithMetadata<RM, PM>) -> HttpMessage,
) {

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
        private val K_LOGGER = KotlinLogging.logger {}

        private fun makeRequestDescription(name: String, id: String, parameters: Map<String, List<String>>): String {
            return "'$name ($id)' request with parameters '$parameters'"
        }
    }

    private val jacksonMapper = applicationContext.jacksonMapper
    private val checkRequestAliveDelay = applicationContext.configuration.checkRequestsAliveDelay.value.toLong()
    private val configuration = applicationContext.configuration

    private class Timeouts {
        class Config(var requestTimeout: Long = 5000L, var excludes: List<String> = listOf("sse"))

        companion object : BaseApplicationPlugin<ApplicationCallPipeline, Config, Unit> {
            override val key: AttributeKey<Unit> = AttributeKey("Timeouts")

            override fun install(pipeline: ApplicationCallPipeline, configure: Config.() -> Unit) {
                val config = Config().apply(configure)
                val timeout = config.requestTimeout
                val excludes = config.excludes

                if (timeout <= 0) return

                pipeline.intercept(ApplicationCallPipeline.Plugins) {
                    if (excludes.any { call.request.uri.contains(it) }) return@intercept
                    withTimeout(timeout) {
                        proceed()
                    }
                }
            }
        }
    }

    @InternalAPI
    suspend fun checkContext(context: ApplicationCall) {
        val flag = AtomicBoolean(false)
        context.attributes.put(AbortableRequestHandler.ABORT_HANDLER_KEY) {
            flag.set(true)
        }

        while (coroutineContext.isActive) {
            if (flag.get()) {
                K_LOGGER.debug { "Aborting coroutine context" }
                throw ClosedChannelException()
            }
            delay(checkRequestAliveDelay)
        }
        K_LOGGER.debug { "Coroutine context closed" }
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
    @InternalAPI
    private suspend fun handleRequest(
        call: ApplicationCall,
        requestId: String,
        cacheControl: CacheControl?,
        probe: Boolean,
        useSse: Boolean,
        requestDescription: String,
        calledFun: suspend () -> Any
    ) {
        coroutineScope {
            try {
                if (useSse) sseRequestGet.inc() else restRequestGet.inc()
                try {
                    if (useSse) {
                        val function = calledFun.invoke()
                        @Suppress("UNCHECKED_CAST")
                        handleSseRequest(call, requestId, function as suspend (StreamWriter<RM, PM>) -> Unit)
                    } else {
                        handleRestApiRequest(call, requestId, cacheControl, probe, calledFun)
                    }
                } catch (e: Exception) {
                    throw e.rootCause ?: e
                } finally {
                    if (useSse) sseRequestProcessed.inc() else restRequestProcessed.inc()
                }
            } catch (e: CancellationException) {
                K_LOGGER.debug(e) { "request $requestDescription processing was cancelled with CancellationException" }
            } catch (e: InvalidRequestException) {
                K_LOGGER.error(e) { "unable to handle request $requestDescription - invalid request" }
            } catch (e: CradleObjectNotFoundException) {
                K_LOGGER.error(e) { "unable to handle request $requestDescription - event or message is missing" }
            } catch (e: CodecResponseException) {
                K_LOGGER.error(e) { "unable to handle request $requestDescription - codec was unable to decode a message" }
            } catch (e: ClosedChannelException) {
                K_LOGGER.debug { "request $requestDescription has been cancelled by a client" }
            } catch (e: CradleIdException) {
                K_LOGGER.error(e) { "unable to handle request $requestDescription - invalid id format" }
            } catch (e: Exception) {
                K_LOGGER.error(e) { "unable to handle request $requestDescription - unexpected exception" }
            }
        }
    }


    @ExperimentalCoroutinesApi
    @InternalAPI
    private suspend fun handleSseRequest(
        call: ApplicationCall,
        requestId: String,
        calledFun: suspend (StreamWriter<RM, PM>) -> Unit
    ) {

        coroutineScope {
            measure("handleSseRequest", requestId) {
                launch {
                    val job = launch {
                        checkContext(call)
                    }
                    call.response.headers.append(HttpHeaders.CacheControl, "no-cache, no-store, no-transform")
                    call.respondTextWriter(contentType = ContentType.Text.EventStream) {
                        val httpWriter = HttpWriter(this, jacksonMapper, convertFun)

                        try {
                            calledFun.invoke(httpWriter)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            httpWriter.eventWrite(SseEvent.build(jacksonMapper, e))
                            throw e
                        } finally {
                            kotlin.runCatching {
                                httpWriter.eventWrite(SseEvent(event = EventType.CLOSE))
                                httpWriter.closeWriter()
                                job.cancel()
                            }.onFailure { e ->
                                K_LOGGER.error(e) { "unexpected exception while trying to close http writer" }
                            }
                        }
                    }
                }.join()
            }
        }
    }

    @ExperimentalCoroutinesApi
    @InternalAPI
    private suspend fun handleRestApiRequest(
        call: ApplicationCall,
        requestId: String,
        cacheControl: CacheControl?,
        probe: Boolean,
        calledFun: suspend () -> Any
    ) {

        coroutineScope {
            measure("handleRestApiRequest", requestId) {
                try {
                    launch {
                        launch {
                            checkContext(call)
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
                        is CradleIdException -> sendErrorCodeOrEmptyJson(
                            probe,
                            call,
                            e,
                            HttpStatusCode.InternalServerError
                        )

                        is CodecResponseException -> sendErrorCode(call, exception, HttpStatusCode.InternalServerError)
                        is CancellationException -> Unit
                        else -> sendErrorCode(call, exception as Exception, HttpStatusCode.InternalServerError)
                    }
                    throw e
                }
            }
        }
    }

    @InternalCoroutinesApi
    @FlowPreview
    @ExperimentalCoroutinesApi
    @InternalAPI
    fun run() {

        val notModifiedCacheControl = this.applicationContext.cacheControlNotModified
        val rarelyModifiedCacheControl = this.applicationContext.cacheControlRarelyModified

        val cradleService = this.applicationContext.cradleService

        val eventCache = this.applicationContext.eventCache
        val messageCache = this.applicationContext.messageCache

        val searchEventsHandler = this.applicationContext.searchEventsHandler
        val searchMessagesHandler = this.applicationContext.searchMessagesHandler

        val eventFiltersPredicateFactory = this.applicationContext.eventFiltersPredicateFactory
        val messageFiltersPredicateFactory = this.applicationContext.messageFiltersPredicateFactory

        val getEventsLimit = this.applicationContext.configuration.eventSearchChunkSize.value.toInt()
        embeddedServer(
            factory = Netty,
            configure = {
                connector {
                    port = configuration.port.value.toInt()
                }
                responseWriteTimeoutSeconds = -1
                channelPipelineConfig = {
                    addLast("cancellationDetector", AbortableRequestHandler())
                }
            }
        ) {
            install(Compression)
            install(Timeouts) {
                requestTimeout = applicationContext.timeout
            }
            install(IgnoreTrailingSlash)

            routing {

                get("/event/{id}") {
                    withRequestId { requestId ->
                        val probe = call.parameters["probe"]?.toBoolean() ?: false
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get single event"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(
                                call, requestId, notModifiedCacheControl, probe,
                                false, requestDescription
                            ) { eventCache.getOrPut(call.parameters.getOrFail("id")).convertToEvent() }
                        }
                    }
                }

                get("/events") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get single event"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(
                                call, requestId, notModifiedCacheControl, false,
                                false, requestDescription
                            ) {
                                val ids = parameters["ids"]
                                when {
                                    ids.isNullOrEmpty() ->
                                        throw InvalidRequestException("Ids set must not be empty: $ids")

                                    ids.size > getEventsLimit ->
                                        throw InvalidRequestException("Too many id in request: ${ids.size}, max is: $getEventsLimit")

                                    else -> eventCache.getOrPutMany(ids.toSet()).map { it.convertToEvent() }
                                }
                            }
                        }
                    }
                }

                get("/messageStreams") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get message streams"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(
                                call, requestId,
                                rarelyModifiedCacheControl, probe = false, useSse = false, requestDescription
                            ) {
                                val bookId = call.request.queryParameters.getOrFail("bookId")
                                cradleService.getSessionAliases(BookId(bookId))
                            }
                        }
                    }
                }

                get("/message/{id}") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get single message"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            val probe = call.parameters["probe"]?.toBoolean() ?: false
                            handleRequest(
                                call, requestId,
                                rarelyModifiedCacheControl, probe, false, requestDescription,
                            ) {
                                MessageWithMetadata(messageCache.getOrPut(call.parameters.getOrFail("id"))).let(
                                    convertFun
                                )
                            }
                        }
                    }
                }

                get("search/sse/messages") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "search messages sse"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, true, requestDescription) {
                                val filterPredicate = messageFiltersPredicateFactory.build(parameters)
                                val request = SseMessageSearchRequest(parameters, filterPredicate)
                                SearchMessagesCalledFun(searchMessagesHandler, request)::calledFun
                            }
                        }
                    }
                }

                get("search/sse/events") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "search events sse"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(
                                call,
                                requestId,
                                null,
                                probe = false,
                                useSse = true,
                                requestDescription = requestDescription
                            ) {
                                val filterPredicate = eventFiltersPredicateFactory.build(parameters)
                                val request = SseEventSearchRequest(parameters, filterPredicate)
                                SearchEventsCalledFun<RM, PM>(searchEventsHandler, request, requestId)::calledFun
                            }
                        }
                    }
                }

                get("filters/sse-messages") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get message filters"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                messageFiltersPredicateFactory.getFiltersNames()
                            }
                        }
                    }
                }

                get("filters/sse-events") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get event filters"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                eventFiltersPredicateFactory.getFiltersNames()
                            }
                        }
                    }
                }

                get("filters/sse-messages/{name}") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get message filters"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                messageFiltersPredicateFactory.getFilterInfo(call.parameters.getOrFail("name"))
                            }
                        }
                    }
                }


                get("filters/sse-events/{name}") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "get event filters"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                eventFiltersPredicateFactory.getFilterInfo(call.parameters.getOrFail("name"))
                            }
                        }
                    }
                }

                get("match/event/{id}") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "match event"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                val filterPredicate = eventFiltersPredicateFactory.build(parameters)
                                filterPredicate.apply(eventCache.getOrPut(call.parameters.getOrFail("id")))
                            }
                        }
                    }
                }

                get("/match/message/{id}") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "match message"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                val filterPredicate = messageFiltersPredicateFactory.build(parameters)
                                val message = messageCache.getOrPut(call.parameters.getOrFail("id"))
                                filterPredicate.apply(MessageWithMetadata(message))
                            }
                        }
                    }
                }

                get("/messageIds") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "message ids"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, false, false, requestDescription) {
                                val request = SseMessageSearchRequest(
                                    parameters,
                                    messageFiltersPredicateFactory.getEmptyPredicate(),
                                ).also(SseMessageSearchRequest<*, *>::checkIdsRequest)
                                searchMessagesHandler.getIds(
                                    request,
                                    configuration.messageIdsLookupLimitDays.value.toLong()
                                )
                            }
                        }
                    }
                }

                get("/bookIds") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "book ids"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            handleRequest(call, requestId, null, probe = false, useSse = false, requestDescription) {
                                cradleService.getBookIds()
                            }
                        }
                    }
                }

                get("/scopeIds") {
                    withRequestId { requestId ->
                        val parameters = call.request.queryParameters.toMap()
                        val requestName = "event scopes"
                        val requestDescription = makeRequestDescription(requestName, requestId, parameters)
                        measure(requestName, requestId, requestDescription, Level.INFO) {
                            val book = call.parameters["bookId"]!!
                            handleRequest(call, requestId, null, probe = false, useSse = false, requestDescription) {
                                cradleService.getEventScopes(BookId(book))
                            }
                        }
                    }
                }
            }
        }.start(false)

        K_LOGGER.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }
    }
}
