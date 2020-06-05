package com.exactpro.th2.reportdataprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.RawMessage
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

    constructor(stored: StoredMessage?, parsed: Message?, storedRaw: StoredMessage?) : this(
        bodyBase64 = storedRaw?.content?.let {
            try {
                RawMessage.parseFrom(it)?.body?.let { body -> Base64.getEncoder().encodeToString(body.toByteArray()) }
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error { "unable to unpack raw message (id=${storedRaw.id}) - invalid data (${String(storedRaw.content)})" }

                null
            }
        },

        messageId = storedRaw?.id?.toString() ?: stored?.id.toString(),

        direction = Direction.fromStored(
            storedRaw?.direction ?: stored?.direction ?: com.exactpro.cradle.Direction.FIRST
        ),

        timestamp = storedRaw?.timestamp ?: stored?.timestamp ?: Instant.ofEpochMilli(0),
        sessionId = storedRaw?.streamName ?: stored?.streamName ?: "unknown",
        messageType = parsed?.metadata?.messageType ?: "unknown",

        body = parsed?.let { JsonFormat.printer().print(parsed) }
    )

    constructor(stored: StoredMessage?, rawMessage: StoredMessage?) : this(
        storedRaw = rawMessage,
        stored = stored,
        parsed = stored?.content?.let {
            try {
                Message.parseFrom(it)
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error { "unable to parse message (id=${stored.id}) - invalid data (${String(stored.content)})" }

                null
            }
        }
    )
}
