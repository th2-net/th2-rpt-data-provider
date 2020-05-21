package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.toMap
import kotlinx.coroutines.async
import mu.KotlinLogging

val logger = KotlinLogging.logger {}

val jacksonMapper: ObjectMapper = jacksonObjectMapper()
    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)

fun <T, R> Sequence<T>.optionalFilter(value: R?, filter: (R, Sequence<T>) -> Sequence<T>): Sequence<T> {
    return if (value == null) this else filter(value, this)
}

fun main() {
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

            get("/event/{id}/children") {
                val parentId = StoredTestEventId.fromString(call.parameters["id"])
                val idsOnly = call.request.queryParameters["idsOnly"]?.toBoolean() ?: false

                async {
                    call.respondText(
                        jacksonMapper.writeValueAsString(
                            manager.storage.testEventsParentsLinker.getChildrenIds(parentId)
                                .map {
                                    if (idsOnly) it.toString() else Event(
                                        manager.storage.getTestEvent(it),
                                        manager
                                    )
                                }
                        ),
                        ContentType.Application.Json
                    )
                }
            }

            get("/event/{id}") {
                val id = StoredTestEventId.fromString(call.parameters["id"])

                async {

                    call.respondText(
                        jacksonMapper.writeValueAsString(Event(manager.storage.getTestEvent(id))),
                        ContentType.Application.Json
                    )
                }
            }

            get("/message/{id}") {
                val id = StoredMessageId.fromString(call.parameters["id"])

                async {
                    call.respondText(
                        jacksonMapper.writeValueAsString(Message(manager.storage.getMessage(id))),
                        ContentType.Application.Json
                    )
                }
            }

            get("/search/messages") {
                val request = MessageSearchRequest(call.request.queryParameters.toMap())

                async {

                    val messages = manager.storage.getMessages(
                        StoredMessageFilterBuilder()
                            .let {
                                if (request.stream != null)
                                    it.streamName().isEqualTo(request.stream) else it
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
                                            .contains(StoredTestEventId.fromString(value))
                                    }
                                }
                                .map { if (request.idsOnly) it.id.toString() else Message(it) }
                                .toList()
                        )
                    }
                }

            }

            get("/search/events") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())

                val events = manager.storage.getTestEvents(request.isRootEvent ?: true).asSequence()

                val linker = manager.storage.testEventsMessagesLinker
                async {

                    call.respondText(
                        jacksonMapper.writeValueAsString(
                            events
                                .optionalFilter(request.attachedMessageId) { value, stream ->
                                    stream.filter {
                                        linker.getMessageIdsByTestEventId(it.id)
                                            .contains(StoredMessageId.fromString(value))
                                    }
                                }
                                .optionalFilter(request.timestampFrom) { value, stream ->
                                    stream.filter { it.endTimestamp.isAfter(value) }
                                }
                                .optionalFilter(request.timestampTo) { value, stream ->
                                    stream.filter { it.startTimestamp.isBefore(value) }
                                }
                                .optionalFilter(request.parentEventId) { value, stream ->
                                    stream.filter { it.parentId == StoredTestEventId.fromString(value) }
                                }
                                .optionalFilter(request.name) { value, stream ->
                                    stream.filter { it.name == value }
                                }
                                .optionalFilter(request.type) { value, stream ->
                                    stream.filter { it.type == value }
                                }
                                .map {
                                    if (request.idsOnly) it.id.toString() else Event(it)
                                }
                                .toList()),
                        ContentType.Application.Json
                    )
                }
            }

        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
