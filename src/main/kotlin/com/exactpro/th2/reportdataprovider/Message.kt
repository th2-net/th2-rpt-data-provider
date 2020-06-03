package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.infra.grpc.Message
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging
import java.time.Instant
import java.util.*

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

    constructor(stored: StoredMessage?, parsed: Message?, rawMessage: StoredMessage?) : this(
        bodyBase64 = rawMessage?.content?.let { Base64.getEncoder().encodeToString(it) },
        messageId = rawMessage?.id?.toString() ?: stored?.id.toString(),

        direction = Direction.fromStored(
            rawMessage?.direction ?: stored?.direction ?: com.exactpro.cradle.Direction.FIRST
        ),

        timestamp = rawMessage?.timestamp ?: stored?.timestamp ?: Instant.ofEpochMilli(0),
        sessionId = rawMessage?.streamName ?: stored?.streamName ?: "unknown",
        messageType = parsed?.metadata?.messageType ?: "unknown",

        body = parsed?.let { JsonFormat.printer().print(parsed) }
    )

    constructor(stored: StoredMessage?, rawMessage: StoredMessage?) : this(
        rawMessage = rawMessage,
        stored = stored,
        parsed = stored?.content?.let {
            try {
                Message.parseFrom(it)
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error {
                        "unable to parse message (id=${stored.id}) to 'body' property - invalid data (${String(
                            stored.content
                        )})"
                    }

                null
            }
        }
    )
}
