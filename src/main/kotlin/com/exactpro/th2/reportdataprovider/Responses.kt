package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.infra.grpc.Message
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging
import java.time.Instant
import java.util.*


enum class Direction {
    IN, OUT;

    companion object {
        fun fromStored(stored: com.exactpro.cradle.Direction): Direction? {
            if (stored == com.exactpro.cradle.Direction.FIRST) return IN
            if (stored == com.exactpro.cradle.Direction.SECOND) return OUT
            return null
        }
    }
}

data class Message(
    val type: String = "message",
    val messageId: String,
    val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val messageType: String,

    @JsonRawValue
    val body: String?,

    val bodyBase64: String?
) {
    constructor(stored: StoredMessage, parsed: Message?, rawData: String?) : this(
        bodyBase64 = rawData,
        messageId = stored.id.toString(),
        direction = Direction.fromStored(stored.direction),
        timestamp = stored.timestamp,
        sessionId = stored.streamName,
        messageType = parsed?.metadata?.messageType ?: "unknown",

        body = parsed?.let { JsonFormat.printer().print(parsed) }
    )

    constructor(stored: StoredMessage, raw: StoredMessage) : this(
        rawData = raw.content?.let { Base64.getEncoder().encodeToString(it) },
        stored = stored,
        parsed = stored.content.let {
            try {
                Message.parseFrom(it)
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error { "unable to parse message (id=${stored.id}) to 'body' property - invalid data (${String(stored.content)})" }

                null
            }
        }
    )
}

data class Event(
    val type: String = "event",
    val eventId: String,
    val eventName: String,
    val eventType: String?,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: String?,
    val isSuccessful: Boolean,
    val attachedMessageIds: Set<String>?,
    val childrenIds: Set<String>,

    @JsonRawValue
    val body: String?
) {
    constructor(stored: StoredTestEventWithContent, cradleManager: CradleManager, childrenIds: Set<String>) : this(
        childrenIds = cradleManager.storage.getTestEvents(stored.id)
            ?.map { it.id.toString() }?.toSet()
            ?: Collections.emptySet<String>()
                .union(childrenIds),

        eventId = stored.id.toString(),
        eventName = stored.name,
        eventType = stored.type,
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = stored.parentId?.toString(),
        isSuccessful = stored.isSuccess,

        attachedMessageIds = cradleManager.storage?.testEventsMessagesLinker
            ?.getMessageIdsByTestEventId(stored.id)?.map(Any::toString)?.toSet().orEmpty(),

        body = stored.content.let {
            try {
                val data = String(it).takeUnless(String::isEmpty) ?: "{}"
                jacksonMapper.readTree(data)
                data
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error { "unable to write event content (id=${stored.id}) to 'body' property - invalid data" }

                null
            }
        }
    )
}
