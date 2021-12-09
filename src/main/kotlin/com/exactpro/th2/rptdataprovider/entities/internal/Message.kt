/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.google.protobuf.ByteString
import java.time.Instant


data class Message(
    val type: String = "message",
    val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val attachedEventIds: Set<String>,
    val messageBody: List<BodyWrapper>?,
    val rawMessageBody: ByteString?,
    val imageType: String?,
    val id: StoredMessageId
) {

    val messageId: String
        get() = id.toString()


    constructor(
        rawStoredMessage: StoredMessage,
        messageBody: List<BodyWrapper>?,
        rawBody: ByteString?,
        events: Set<String>?,
        imageType: String?
    ) : this(
        id = rawStoredMessage.id,
        direction = Direction.fromStored(rawStoredMessage.direction ?: com.exactpro.cradle.Direction.FIRST),
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        sessionId = rawStoredMessage.streamName ?: "",
        attachedEventIds = events ?: emptySet(),
        rawMessageBody = rawBody,
        messageBody = messageBody,
        imageType = imageType
    )

    private constructor(builder: Builder) : this(
        builder.rawStoredMessage,
        builder.messageBody,
        builder.rawMessage?.body,
        builder.events,
        builder.imageType
    )

    class Builder(val rawStoredMessage: StoredMessage, val rawMessage: RawMessage?) {
        var messageBody: List<BodyWrapper>? = null
            private set

        var events: Set<String>? = null
            private set

        var imageType: String? = null
            private set

        fun parsedMessage(messageBody: List<BodyWrapper>?) = apply { this.messageBody = messageBody }

        fun attachedEvents(events: Set<String>?) = apply { this.events = events }

        fun imageType(imageType: String?) = apply { this.imageType = imageType }

        fun build() = Message(this)
    }

}

