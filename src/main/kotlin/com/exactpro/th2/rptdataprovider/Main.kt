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

import com.exactpro.th2.rptdataprovider.services.cradle.CradleObjectNotFoundException
import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.Compression
import io.ktor.http.CacheControl
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.cacheControl
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.InternalAPI
import io.ktor.util.rootCause
import io.ktor.util.toMap
import kotlinx.coroutines.IO_PARALLELISM_PROPERTY_NAME
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.system.measureTimeMillis

class Main(args: Array<String>) {

    private val logger = KotlinLogging.logger {}

    private val context = Context(args)
    private val configuration = context.configuration
    private val jacksonMapper = context.jacksonMapper
    private val timeout = context.timeout

    @InternalAPI
    private suspend fun handleRequest(
        call: ApplicationCall,
        requestName: String,
        cacheControl: CacheControl?,
        vararg parameters: Any?,
        calledFun: suspend () -> Any
    ) {
        coroutineScope {
            measureTimeMillis {
                logger.debug { "handling '$requestName' request with parameters '$parameters'" }

                try {
                    try {
                        launch {
                            withTimeout(timeout) {
                                cacheControl?.let { call.response.cacheControl(it) }
                                call.respondText(
                                    jacksonMapper.asStringSuspend(calledFun.invoke()), ContentType.Application.Json
                                )
                            }
                        }.join()
                    } catch (e: Exception) {
                        throw e.rootCause ?: e
                    }
                } catch (e: CradleObjectNotFoundException) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$parameters' - missing cradle data" }
                    call.respondText(
                        e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, HttpStatusCode.NotFound
                    )
                } catch (e: Exception) {
                    logger.error(e) { "unable to handle request '$requestName' with parameters '$parameters' - unexpected exception" }
                    call.respondText(
                        e.rootCause?.message ?: e.toString(), ContentType.Text.Plain, HttpStatusCode.InternalServerError
                    )
                }
            }.let { logger.debug { "request '$requestName' with parameters '$parameters' handled - time=${it}ms" } }
        }
    }

    private fun inPast(rightTimeBoundary: Instant?): Boolean {
        return rightTimeBoundary?.isBefore(Instant.now()) != false
    }

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

        System.setProperty(IO_PARALLELISM_PROPERTY_NAME, configuration.ioDispatcherThreadPoolSize.value)

        embeddedServer(Netty, configuration.port.value.toInt()) {

            install(Compression)

            routing {
                get("/") {
                    val startOfDay =
                        LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).atZone(ZoneId.of("UTC"))
                            .toEpochSecond() * 1000

                    val currentTime = LocalDateTime.now().atZone(ZoneId.of("UTC")).toEpochSecond() * 1000

                    call.respondText(
                        """
                        <h1>Report data provider is working.</h1>
                        <div>Cassandra endpoint is set to <u><pre style="display: inline">${configuration.cassandraHost.value}:${configuration.cassandraPort.value}</pre></u>.</div>
                        <div>Keyspace is set to <pre style="display: inline">${configuration.cassandraKeyspace.value}</pre></div>
                        <a href="search/events?timestampFrom=${startOfDay}&timestampTo=${currentTime}">list of events since the start of day (json)</a>
                        <div>Check API reference for details.</div>
                        """.trimIndent(),
                        ContentType.Text.Html
                    )

                }

                get("/event/{id}") {
                    val id = call.parameters["id"]

                    handleRequest(call, "get single event", notModifiedCacheControl, id) {
                        eventCache.getOrPut(id!!)
                    }
                }

                get("/messageStreams") {
                    handleRequest(call, "get message streams", rarelyModifiedCacheControl) {
                        cradleService.getMessageStreams()
                    }
                }

                get("/message/{id}") {
                    val id = call.parameters["id"]

                    handleRequest(call, "get single message", notModifiedCacheControl, id) {
                        messageCache.getOrPut(id!!)
                    }
                }

                get("/search/messages") {
                    val request = MessageSearchRequest(call.request.queryParameters.toMap())

                    handleRequest(call, "search messages", null, request) {
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

                    handleRequest(call, "search events", null, request) {
                        searchEventsHandler.searchEvents(request)
                            .also {
                                call.response.cacheControl(
                                    if (inPast(request.timestampTo)) {
                                        notModifiedCacheControl
                                    } else {
                                        frequentlyModifiedCacheControl
                                    }
                                )
                            }
                    }
                }
            }
        }.start(false)

        logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

    }
}

@InternalAPI
fun main(args: Array<String>) {
    Main(args).run()
}
