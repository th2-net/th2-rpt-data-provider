package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.Direction
import java.time.Instant

data class MessageWrapper(
    val id: StoredMessageId,
    val rawMessage: RawMessage,
    val sessionId: String,
    val direction: Direction,
    val timestamp: Instant
) {
    constructor(
        rawStoredMessage: StoredMessage,
        rawMessage: RawMessage
    ) : this(
        id = rawStoredMessage.id,
        rawMessage = rawMessage,
        direction = Direction.fromStored(rawStoredMessage.direction ?: com.exactpro.cradle.Direction.FIRST),
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        sessionId = rawStoredMessage.streamName ?: ""
    )
}