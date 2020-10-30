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

import com.exactpro.th2.rptdataprovider.entities.requests.EventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.requests.MessageSearchRequest
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.Compression
import io.ktor.http.*
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.system.measureTimeMillis

private fun inPast(rightTimeBoundary: Instant?): Boolean {
    return rightTimeBoundary?.isBefore(Instant.now()) != false
}

@InternalAPI
fun main() {
    val logger = KotlinLogging.logger {}

    val context = Context()
    val configuration = context.configuration
    val jacksonMapper = context.jacksonMapper
    val timeout = context.timeout


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

                logger.debug { "handling get event request with id=$id" }

                measureTimeMillis {
                    try {
                        launch {
                            withTimeout(timeout) {
                                call.response.cacheControl(context.cacheControlNotModified)
                                try {
                                    call.respondText(
                                        jacksonMapper.asStringSuspend(context.eventCache.getOrPut(id!!)),
                                        ContentType.Application.Json
                                    )
                                } catch (e: IllegalArgumentException) {
                                    logger.error(e) { "Event id=$id not found" }
                                    call.respondText(
                                        e.rootCause?.message ?: e.toString(),
                                        ContentType.Text.Plain,
                                        HttpStatusCode.NotFound
                                    )
                                } catch (e: Exception) {
                                    throw e
                                }
                            }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve event with id=$id" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "get event handled - time=${it}ms id=$id" } }
            }

            get("/messageStreams") {
                measureTimeMillis {
                    try {
                        launch {
                            withTimeout(timeout) {
                                call.response.cacheControl(context.cacheControlRarelyModified)

                                call.respondText(
                                    jacksonMapper.asStringSuspend(context.cradleService.getMessageStreams()),
                                    ContentType.Application.Json
                                )
                            }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve message streams" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "get message streams handled - time=${it}ms" } }
            }

            get("/message/{id}") {
                val id = call.parameters["id"]

                logger.debug { "handling get message request with id=$id" }

                measureTimeMillis {
                    try {
                        call.response.cacheControl(context.cacheControlNotModified)

                        context.messageCache.getOrPut(id!!).let {
                            call.respondText(
                                jacksonMapper.asStringSuspend(it),
                                ContentType.Application.Json
                            )
                        }
                    } catch (e: IllegalArgumentException) {
                        logger.error(e) { "Message id=$id not found" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.NotFound
                        )
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve message with id=$id" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "get message handled - time=${it}ms id=$id" } }
            }

            get("/search/messages") {
                val request =
                    MessageSearchRequest(call.request.queryParameters.toMap())

                logger.debug { "handling search messages request (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            context.searchMessagesHandler.searchMessages(request)
                                .let {
                                    call.response.cacheControl(
                                        if (it.size == request.limit || inPast(request.timestampTo)) {
                                            context.cacheControlNotModified
                                        } else {
                                            context.cacheControlFrequentlyModified
                                        }
                                    )
                                    call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
                                        jacksonMapper.asStringSuspend(it)
                                    }
                                }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search messages - unexpected exception" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "message search handled - time=${it}ms request=$request" } }
            }

            get("search/events") {
                val request =
                    EventSearchRequest(call.request.queryParameters.toMap())

                logger.debug { "handling search events request with (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            call.response.cacheControl(
                                if (inPast(request.timestampTo)) {
                                    context.cacheControlNotModified
                                } else {
                                    context.cacheControlFrequentlyModified
                                }
                            )

                            call.respondText(
                                jacksonMapper.asStringSuspend(
                                    context.searchEventsHandler.searchEvents(request)
                                ),
                                ContentType.Application.Json
                            )
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events with request=$request" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "search events handled - time=${it}ms request=$request" } }
            }
        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
