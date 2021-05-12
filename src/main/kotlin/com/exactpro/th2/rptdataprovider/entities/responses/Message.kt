/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.Direction
import com.exactpro.th2.rptdataprovider.grpc.RptMessage
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonRawValue
import java.time.Instant

data class Message(
    val type: String = "message",
    val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val messageType: String,

    @JsonRawValue
    val body: String?,

    val bodyBase64: String?,

    @JsonIgnore
    val id: StoredMessageId
) {

    val messageId: String
        get() = id.toString()


    constructor(rawStoredMessage: StoredMessage, jsonBody: String?, base64Body: String?, messageType: String) : this(
        bodyBase64 = base64Body,
        body = jsonBody,
        messageType = messageType,
        id = rawStoredMessage.id,
        direction = Direction.fromStored(rawStoredMessage.direction ?: com.exactpro.cradle.Direction.FIRST),
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        sessionId = rawStoredMessage.streamName ?: ""
    )


    private fun getMessageId(): MessageID {
        return MessageID.newBuilder()
            .setSequence(id.index)
            .setDirection(if (id.direction == com.exactpro.cradle.Direction.FIRST) FIRST else SECOND)
            .setConnectionId(ConnectionID.newBuilder()
                .setSessionAlias(id.streamName))
            .build()
    }

    fun convertToGrpcRptMessage(): RptMessage {
        return RptMessage.newBuilder()
            .setMessageId(getMessageId())
            .setTimestamp(timestamp.convertToProto())
            .setDirection(if (id.direction == com.exactpro.cradle.Direction.FIRST) FIRST else SECOND)
            .setSessionId(ConnectionID.newBuilder().setSessionAlias(id.streamName))
            .setMessageType(messageType)
            .let { builder ->
                body?.let { builder.setBody(body) }
                bodyBase64?.let { builder.setBodyBase64(bodyBase64) }
                builder
            }
            .build()
    }
}
