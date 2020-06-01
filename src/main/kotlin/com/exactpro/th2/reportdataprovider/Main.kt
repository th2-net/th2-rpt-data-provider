package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.testevents.StoredTestEventId
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.toMap
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.nio.file.Paths
import java.time.ZoneId
import java.time.format.DateTimeFormatter

val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.nnnnnnnnn").withZone(ZoneId.of("UTC"))

val jacksonMapper: ObjectMapper = jacksonObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
//    .registerModule(
//        JavaTimeModule()
//            .addSerializer(Instant::class.java, InstantSerializer())
//            .addDeserializer(Instant::class.java, InstantDeserializer())
//    )

fun main() {
    val logger = KotlinLogging.logger {}

    val configuration = Configuration()

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

    manager.init(configuration.cassandraInstance.value)

    embeddedServer(Netty, configuration.port.value.toInt()) {
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

                launch {
                    try {
                        val path = Paths.get(pathString!!)

                        logger.debug { "handling get event request with path=$path" }

                        call.respondText(
                            jacksonMapper.writeValueAsString(eventCache.getEvent(path)), ContentType.Application.Json
                        )
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve event with path=$pathString" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }
            }

            get("/message/{id}") {
                val id = call.parameters["id"]

                launch {
                    try {
                        call.respondText(
                            jacksonMapper.writeValueAsString(messageCache.get(id!!)), ContentType.Application.Json
                        )
                    } catch (e: Exception) {
                        logger.error(e) { "unable to retrieve message with id=$id" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }
            }

            get("/search/messages") {
                val request = MessageSearchRequest(call.request.queryParameters.toMap())

                launch {
                    try {
                        val messages = manager.storage.getMessages(
                            StoredMessageFilterBuilder()
                                .let {
                                    if (request.stream != null)
                                        it.streamName().isEqualTo(request.stream.first()) else it
                                }
                                .let {
                                    if (request.timestampFrom != null)
                                        it.timestampFrom().isGreaterThanOrEqualTo(request.timestampFrom) else it
                                }
                                .let {
                                    if (request.timestampTo != null)
                                        it.timestampTo().isLessThanOrEqualTo(request.timestampTo) else it
                                }
                                .build()
                        ).asSequence<StoredMessage>()

                        val linker = manager.storage.testEventsMessagesLinker

                        call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
                            jacksonMapper.writeValueAsString(
                                messages
                                    .optionalFilter(request.attachedEventId) { value, stream ->
                                        stream.filter {
                                            linker.getTestEventIdsByMessageId(it.id)
                                                .contains(StoredTestEventId(value))
                                        }
                                    }
                                    .map { Message(manager.storage.getProcessedMessage(it.id), it) }
                                    .optionalFilter(request.messageType) { value, stream ->
                                        stream.filter {
                                            value.contains(it.messageType)
                                        }
                                    }
                                    .sortedBy { it.timestamp }
                                    .map { if (request.idsOnly) it.messageId else it }
                                    .toList()
                            )
                        }
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search messages - unexpected exception" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }

            }

            get("/rootEvents") {
                launch {
                    try {
                        call.respondText(
                            jacksonMapper.writeValueAsString(manager.storage.rootTestEvents.map {
                                eventCache.getEvent(
                                    Paths.get(it.id.toString())
                                )
                            }),
                            ContentType.Application.Json
                        )
                    } catch (e: Exception) {
                        logger.error(e) { "unable to search events - unexpected exception" }
                        call.respond(HttpStatusCode.InternalServerError, e.message ?: "")
                    }
                }
            }

        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
