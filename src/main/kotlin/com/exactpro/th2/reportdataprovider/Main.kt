package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.th2.reportdataprovider.cache.EventCacheManager
import com.exactpro.th2.reportdataprovider.cache.MessageCacheManager
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
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.toMap
import kotlinx.coroutines.IO_PARALLELISM_PROPERTY_NAME
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.system.measureTimeMillis


val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.nnnnnnnnn").withZone(ZoneId.of("UTC"))

val jacksonMapper: ObjectMapper = jacksonObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

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
                call.respondText(
                    """
                        <h1>Report data provider is working.</h1>
                        <div>Cassandra endpoint is set to <u><pre style="display: inline">${configuration.cassandraHost.value}:${configuration.cassandraPort.value}</pre></u>.</div>
                        <a href="search/events">list of root events (json)</a>
                        <div>Check API reference for details.</div>
                    """.trimIndent(),
                    ContentType.Text.Html
                )
            }

            get("/event/{path...}") {
                val pathString = call.parameters.getAll("path")?.joinToString("/")

                logger.debug { "handling get event request with path=$pathString" }

                measureTimeMillis {
                    try {
                        launch {
                            withTimeout(timeout) {
                                call.response.cacheControl(cacheControl)

                                call.respondText(
                                    jacksonMapper.asStringSuspend(eventCache.getOrPut(pathString!!)),
                                    ContentType.Application.Json
                                )
                            }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve event with path=$pathString" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }.let { logger.debug { "get event request took $it milliseconds" } }

            }

            get("/message/{id}") {
                val id = call.parameters["id"]

                logger.debug { "handling get message request with id=$id" }
                try {
                    call.response.cacheControl(cacheControl)

                    call.respondText(
                        jacksonMapper.asStringSuspend(messageCache.getOrPut(id!!)), ContentType.Application.Json
                    )
                } catch (e: Exception) {
                    logger.error(e) { "unable to retrieve message with id=$id" }
                    call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                }
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
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }.let { logger.debug { "message search request took $it milliseconds" } }
            }

            get("search/events/{path...}") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())
                val pathString = call.parameters.getAll("path")?.joinToString("/")

                logger.debug { "handling search events request with path=$pathString (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            call.response.cacheControl(cacheControl)

                            call.respondText(
                                jacksonMapper.asStringSuspend(
                                    if (pathString.isNullOrEmpty()) {
                                        getRootEvents(request, manager, eventCache, timeout)
                                    } else {
                                        searchChildrenEvents(request, pathString, eventCache, timeout)
                                    }
                                ),
                                ContentType.Application.Json
                            )
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events with path=$pathString" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }.let { logger.debug { "search events request took $it milliseconds" } }
            }

            get("/rootEvents") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())

                logger.debug { "handling get root events request (query=$request)" }

                measureTimeMillis {
                    try {
                        launch {
                            getRootEvents(request, manager, eventCache, timeout).let {
                                call.response.cacheControl(cacheControl)
                                call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
                                    jacksonMapper.asStringSuspend(it)
                                }
                            }
                        }.join()
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events - unexpected exception" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }.let { logger.debug { "root events request took $it milliseconds" } }
            }

        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
