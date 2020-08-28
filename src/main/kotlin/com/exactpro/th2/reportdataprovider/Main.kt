/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
import com.exactpro.th2.reportdataprovider.entities.EventSearchRequest
import com.exactpro.th2.reportdataprovider.entities.MessageSearchRequest
import com.exactpro.th2.reportdataprovider.handlers.getRootEvents
import com.exactpro.th2.reportdataprovider.handlers.searchChildrenEvents
import com.exactpro.th2.reportdataprovider.handlers.searchMessages
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.system.measureTimeMillis


val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.nnnnnnnnn").withZone(ZoneId.of("UTC"))

val jacksonMapper: ObjectMapper = jacksonObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

@InternalAPI
fun main() {
    val logger = KotlinLogging.logger {}
    val configuration = Configuration()

    System.setProperty(IO_PARALLELISM_PROPERTY_NAME, configuration.ioDispatcherThreadPoolSize.value)

    val manager = CassandraCradleManager(CassandraConnection(configuration.let {
        val settings = CassandraConnectionSettings(
            it.cassandraDatacenter.value,
            it.cassandraHost.value,
            it.cassandraPort.value.toInt(),
            it.cassandraKeyspace.value
        )

        settings.timeout = it.cassandraQueryTimeout.value.toLong()
        settings.username = it.cassandraUsername.value
        settings.password = it.cassandraPassword.value

        settings
    }))

    val eventCache = EventCacheManager(configuration, manager)
    val messageCache = MessageCacheManager(configuration, manager)
    val timeout: Long = configuration.responseTimeout.value.toLong()

    val cacheControl = configuration.clientCacheTimeout.value.toInt().let {
        CacheControl.MaxAge(
            visibility = CacheControl.Visibility.Public,
            maxAgeSeconds = it,
            mustRevalidate = false,
            proxyRevalidate = false,
            proxyMaxAgeSeconds = it
        )
    }

    manager.init(configuration.cassandraInstance.value)

    embeddedServer(Netty, configuration.port.value.toInt()) {

        install(Compression)

        routing {
            get("/") {
                val startOfDay =
                    LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).atZone(ZoneId.of("UTC")).toEpochSecond() * 1000
                val currentTime = LocalDateTime.now().atZone(ZoneId.of("UTC")).toEpochSecond() * 1000

                call.respondText(
                    """
                        <h1>Report data provider is working.</h1>
                        <div>Cassandra endpoint is set to <u><pre style="display: inline">${configuration.cassandraHost.value}:${configuration.cassandraPort.value}</pre></u>.</div>
                        <div>Keyspace is set to <pre style="display: inline">${configuration.cassandraKeyspace.value}</pre></div>
                        <a href="rootEvents?timestampFrom=${startOfDay}&timestampTo=${currentTime}">list of root events (json)</a>
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
                                call.response.cacheControl(cacheControl)

                                call.respondText(
                                    jacksonMapper.asStringSuspend(eventCache.getOrPut(id!!)),
                                    ContentType.Application.Json
                                )
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
                                call.response.cacheControl(cacheControl)

                                call.respondText(
                                    jacksonMapper.asStringSuspend(manager.storage.streams),
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
                        call.response.cacheControl(cacheControl)

                        call.respondText(
                            jacksonMapper.asStringSuspend(messageCache.getOrPut(id!!)), ContentType.Application.Json
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
                val request = MessageSearchRequest(call.request.queryParameters.toMap())

                logger.debug { "handling search messages request (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            searchMessages(request, manager, messageCache, timeout)
                                .let {
                                    call.response.cacheControl(cacheControl)
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

            get("search/events/{id}") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())
                val id = call.parameters["id"]!!

                logger.debug { "handling search events request with parentId=$id (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            call.response.cacheControl(cacheControl)

                            call.respondText(
                                jacksonMapper.asStringSuspend(
                                    searchChildrenEvents(request, id, eventCache, manager, timeout)
                                        .map { it.toString() }
                                ),
                                ContentType.Application.Json
                            )
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events with parentId=$id" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "search events handled - time=${it}ms request=$request parentId=$id" } }
            }

            get("/rootEvents") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())

                logger.debug { "handling get root events request (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            getRootEvents(request, manager, timeout).let {
                                call.response.cacheControl(cacheControl)
                                call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
                                    jacksonMapper.asStringSuspend(it)
                                }
                            }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events - unexpected exception" }
                        call.respondText(
                            e.rootCause?.message ?: e.toString(),
                            ContentType.Text.Plain,
                            HttpStatusCode.InternalServerError
                        )
                    }
                }.let { logger.debug { "get root events handled - time=${it}ms request=$request" } }
            }
        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
