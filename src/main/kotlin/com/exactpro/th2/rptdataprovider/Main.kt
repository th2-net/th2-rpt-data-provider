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

package com.exactpro.th2.rptdataprovider

import com.exactpro.cradle.utils.CradleIdException
import com.exactpro.th2.rptdataprovider.entities.exceptions.ChannelClosedException
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.sse.SseEvent
import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.util.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.nio.channels.ClosedChannelException
import java.time.Instant
import kotlin.coroutines.coroutineContext
import kotlin.system.measureTimeMillis

class Main(args: Array<String>) {

    private val logger = KotlinLogging.logger {}

    private val context = Context(args)
    private val jacksonMapper = context.jacksonMapper
    private val checkRequestAliveDelay = context.configuration.checkRequestsAliveDelay.value.toLong()
    private val configuration = context.configuration

    private class Timeouts {
        class Config(var requestTimeout: Long = 5000L, var excludes: List<String> = listOf("sse"))

        companion object : ApplicationFeature<ApplicationCallPipeline, Config, Unit> {
            override val key: AttributeKey<Unit> = AttributeKey("Timeouts")

            override fun install(pipeline: ApplicationCallPipeline, configure: Config.() -> Unit) {
                val timeout = Config().apply(configure).requestTimeout
                val excludes = Config().apply(configure).excludes

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
                    throw ChannelClosedException("Channel is closed")

                delay(checkRequestAliveDelay)
            }
        }
    }

    @InternalAPI
    private suspend fun sendErrorCodeOrEmptyJson(
        probe: Boolean,
        call: ApplicationCall,
        e: Exception,
        code: HttpStatusCode
    ) {
        if (probe) {
            call.respondText(
                jacksonMapper.writeValueAsString(null), ContentType.Application.Json
            )
        } else {
            call.respondText(
                e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, code
            )
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
        val stringParameters = parameters.contentDeepToString()
        coroutineScope {
            measureTimeMillis {
                logger.debug { "handling '$requestName' request with parameters '$stringParameters'" }
                try {
                    try {
                        launch {
                            launch {
                                checkContext(context)
                            }
                            if (useSse) {
                                calledFun.invoke()
                            } else {
                                cacheControl?.let { call.response.cacheControl(it) }
                                call.respondText(
                                    jacksonMapper.asStringSuspend(calledFun.invoke()),
                                    ContentType.Application.Json
                                )
                            }
                        }.join()
                    } catch (e: Exception) {
                        when (e.rootCause ?: e) {
                            is ChannelClosedException -> if (!useSse) throw e.rootCause ?: e
                            else -> throw e.rootCause ?: e
                        }
                    } finally {
                        coroutineContext.cancelChildren()
                    }
                } catch (e: InvalidRequestException) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - invalid request" }
                    call.respondText(
                        e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, HttpStatusCode.BadRequest
                    )
                } catch (e: CradleObjectNotFoundException) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - missing cradle data" }
                    sendErrorCodeOrEmptyJson(probe, call, e, HttpStatusCode.NotFound)
                } catch (e: ChannelClosedException) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - request closer" }
                    call.respondText(
                        e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, HttpStatusCode.RequestTimeout
                    )
                } catch (e: Exception) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$stringParameters' - unexpected exception" }
                    when (e) {
                        is CradleIdException -> {
                            sendErrorCodeOrEmptyJson(probe, call, e, HttpStatusCode.InternalServerError)
                        }
                        else -> call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }
            }.let { logger.debug { "request '$requestName' with parameters '$stringParameters' handled - time=${it}ms" } }
        }
    }

    private fun inPast(rightTimeBoundary: Instant?): Boolean {
        return rightTimeBoundary?.isBefore(Instant.now()) != false
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    @EngineAPI
    @InternalAPI
    fun run() {

        val notModifiedCacheControl = this.context.cacheControlNotModified
        val rarelyModifiedCacheControl = this.context.cacheControlRarelyModified
        val frequentlyModifiedCacheControl = this.context.cacheControlFrequentlyModified

        val cradleService = this.context.cradleService

        val eventCache = this.context.eventCache
        val messageCache = this.context.messageCache

        val searchEventsHandler = this.context.searchEventsHandler
        val searchMessagesHandler = this.context.searchMessagesHandler


        val sseEventSearchStep = this.context.sseEventSearchStep

        System.setProperty(IO_PARALLELISM_PROPERTY_NAME, configuration.ioDispatcherThreadPoolSize.value)

        embeddedServer(Netty, configuration.port.value.toInt()) {

            install(Compression)
            install(Timeouts) {
                requestTimeout = context.timeout
            }

            routing {

                get("/event/{id}") {
                    val id = call.parameters["id"]
                    val probe = call.parameters["probe"]?.toBoolean() ?: false

                    handleRequest(call, context, "get single event", notModifiedCacheControl, probe, false, id) {
                        eventCache.getOrPut(id!!)
                    }
                }

                get("/messageStreams") {
                    handleRequest(
                        call, context, "get message streams", rarelyModifiedCacheControl,
                        probe = false,
                        useSse = false
                    ) {
                        cradleService.getMessageStreams()
                    }
                }

                get("/message/{id}") {
                    val id = call.parameters["id"]
                    val probe = call.parameters["probe"]?.toBoolean() ?: false

                    handleRequest(call, context, "get single message", notModifiedCacheControl, probe, false, id) {
                        messageCache.getOrPut(id!!)
                    }
                }

                get("/search/messages") {
                    val request = MessageSearchRequest(call.request.queryParameters.toMap())

                    handleRequest(call, context, "search messages", null, request.probe, false, request) {
                        searchMessagesHandler.searchMessages(request)
                            .also {
                                call.response.cacheControl(
                                    if (it.size == request.limit || inPast(request.timestampTo)) {
                                        notModifiedCacheControl
                                    } else {
                                        frequentlyModifiedCacheControl
                                    }
                                )
                            }
                    }
                }

                get("search/events") {
                    val request = EventSearchRequest(call.request.queryParameters.toMap())

                    handleRequest(call, context, "search events", null, request.probe, false, request) {
                        searchEventsHandler.searchEvents(request)
                            .also { call.response.cacheControl(frequentlyModifiedCacheControl) }
                    }
                }

                get("search/sse/events") {
                    val request = SseEventSearchRequest(call.request.queryParameters.toMap())
                    handleRequest(call, context, "search events sse", null, false, true, request) {
                        call.response.cacheControl(CacheControl.NoCache(null))
                        searchEventsHandler.searchEventsSse(request, call, jacksonMapper, sseEventSearchStep)
                    }
                }
            }
        }.start(false)

        logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

    }
}

@FlowPreview
@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    Main(args).run()
}
