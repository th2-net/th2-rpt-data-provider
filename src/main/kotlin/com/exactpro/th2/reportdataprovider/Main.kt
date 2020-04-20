package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.toMap
import mu.KotlinLogging


private val logger = KotlinLogging.logger {}

fun main() {
    val configuration = Configuration()

    val manager = CassandraCradleManager(CassandraConnection(configuration.let {
        val settings = CassandraConnectionSettings(
            it.cassandraDatacenter.value,
            it.cassandraHost.value,
            it.cassandraPort.value.toInt(),
            it.cassandraKeyspace.value
        )

        settings.username = it.cassandraUsername.value
        settings.password = it.cassandraPassword.value

        settings
    }))

    manager.init(configuration.cassandraInstance.value)

    val jsonMapper = jacksonObjectMapper()

    fun json(data: Any): String {
        return jsonMapper.writeValueAsString(data)
    }

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

                call.respondText(
                    json(
                        manager.storage.testEventsParentsLinker.getChildrenIds(parentId)
                            .map { if (idsOnly) it.toString() else Event(manager.storage.getTestEvent(it)) }
                    ),
                    ContentType.Application.Json
                )
            }

            get("/event/{id}") {
                val id = StoredTestEventId.fromString(call.parameters["id"])

                call.respondText(
                    json(Event(manager.storage.getTestEvent(id))),
                    ContentType.Application.Json
                )
            }

            get("/message/{id}") {
                val id = StoredMessageId.fromString(call.parameters["id"])

                call.respondText(
                    json(Message(manager.storage.getMessage(id))),
                    ContentType.Application.Json
                )
            }

            get("/search/messages") {
                val request = MessageSearchRequest(call.request.queryParameters.toMap())

                call.respondText(
                    json(manager.storage.messages
                        .filter {
                            request.attachedEventId
                                ?.equals(manager.storage.testEventsMessagesLinker.getTestEventIdsByMessageId(it.id))
                                    ?: true

                                    && request.timestampFrom?.isBefore(it.timestamp) ?: true
                                    && request.timestampTo?.isAfter(it.timestamp) ?: true
                                    && request.stream?.equals(it.streamName) ?: true
                        }
                        .map { if (request.idsOnly) it.id.toString() else Message(it) }),
                    ContentType.Application.Json
                )
            }

            get("/search/events") {
                val request = EventSearchRequest(call.request.queryParameters.toMap())

                call.respondText(
                    json(manager.storage.getTestEvents(request.isRootEvent ?: true)
                        .filter {
                            request.attachedMessageId
                                ?.equals(manager.storage.testEventsMessagesLinker.getMessageIdsByTestEventId(it.id))
                                    ?: true

                                    && request.timestampFrom?.isBefore(it.endTimestamp) ?: true
                                    && request.timestampTo?.isAfter(it.startTimestamp) ?: true
                                    && request.parentEventId?.equals(it.parentId) ?: true
                                    && request.name?.equals(it.name) ?: true
                                    && request.type?.equals(it.type) ?: true

                        }
                        .map { if (request.idsOnly) it.id.toString() else Event(it) }),
                    ContentType.Application.Json
                )
            }

        }
    }.start(false)

    logger.info { "serving on: http://${configuration.hostname.value}:${configuration.port.value}" }

}
