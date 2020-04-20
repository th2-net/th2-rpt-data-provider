package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.evolution.api.phase_1.Message
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.util.JsonFormat
import java.time.Instant

enum class Direction {
    IN, OUT;

    companion object {
        fun fromStored(stored: com.exactpro.cradle.Direction): Direction? {
            if (stored == com.exactpro.cradle.Direction.RECEIVED) return IN
            if (stored == com.exactpro.cradle.Direction.SENT) return OUT
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

    @JsonRawValue
    val body: String
) {
    constructor(stored: StoredMessage) : this(
        messageId = stored.id.toString(),
        direction = Direction.fromStored(stored.direction),
        timestamp = stored.timestamp,
        sessionId = stored.streamName,
        body = JsonFormat.printer().print(Message.parseFrom(stored.content))
    )
}

data class Event(
    val type: String = "event",
    val eventId: String,
    val eventName: String,
    val eventType: String,
    val endTimestamp: Instant,
    val startTimestamp: Instant,
    val parentEventId: String,
    val isSuccessful: Boolean,
    val attachedMessageIds: Set<String>,

    @JsonRawValue
    val body: String
) {
    constructor(stored: StoredTestEvent, cradleManager: CradleManager? = null) : this(
        eventId = stored.id.toString(),
        eventName = stored.name,
        eventType = stored.type,
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = stored.parentId.toString(),
        isSuccessful = stored.isSuccess,

        attachedMessageIds = cradleManager?.storage?.testEventsMessagesLinker
            ?.getMessageIdsByTestEventId(stored.id)?.map(Any::toString)?.toSet().orEmpty(),

        body = String(stored.content)
    )
}
