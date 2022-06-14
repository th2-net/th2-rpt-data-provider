/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.util.toCradleDirection
import com.exactpro.th2.dataprovider.grpc.EventStreamPointer
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.internal.StreamName
import com.fasterxml.jackson.annotation.JsonIgnore
import mu.KotlinLogging

data class EventStreamPointer(
    val hasStarted: Boolean,
    val hasEnded: Boolean,
    val lastId: ProviderEventId? = null
) {
    companion object {
        val logger = KotlinLogging.logger { }
    }

    fun convertToProto(): EventStreamPointer {
        return EventStreamPointer.newBuilder()
            .setHasEnded(hasEnded)
            .setHasStarted(hasStarted)
            .also { builder ->
                lastId?.let { builder.setLastId(EventID.newBuilder().setId(it.toString())) }
            }.build()
    }
}


data class MessageStreamPointer(
    @JsonIgnore
    val messageStream: StreamName,
    val hasStarted: Boolean,
    val hasEnded: Boolean,
    @JsonIgnore
    val lastMessageId: StoredMessageId? = null
) {

    @JsonIgnore
    private val lastMessage = let {
        if (lastMessageId == null || hasEnded) {
            // set sequence to a negative value if stream has not started to avoid mistaking it for the default value
            // FIXME change interface to make that case explicit
            logger.trace { "lastElement is null - StreamInfo should be build as if the stream $messageStream has no messages" }
            StoredMessageId(messageStream.name, messageStream.direction, -1)
        } else {
            lastMessageId
        }
    }

    val lastId = lastMessage.toString()

    companion object {
        val logger = KotlinLogging.logger { }
    }

    constructor(lastId: MessageID, messageStream: StreamName, hasStarted: Boolean, hasEnded: Boolean) : this(
        lastMessageId = StoredMessageId(
            lastId.connectionId.sessionAlias,
            lastId.direction.toCradleDirection(),
            lastId.sequence
        ),
        messageStream = messageStream,
        hasStarted = hasStarted,
        hasEnded = hasEnded
    )

    fun convertToProto(): MessageStreamPointer {
        return MessageStreamPointer.newBuilder()
            .setMessageStream(
                MessageStream.newBuilder()
                    .setDirection(cradleDirectionToGrpc(messageStream.direction))
                    .setName(messageStream.name)
            )
            .setHasStarted(hasStarted)
            .setHasEnded(hasEnded)
            .setLastId(

                lastMessage.let {
                    logger.trace { "stream $messageStream - lastElement is ${it.index}" }
                    it.convertToProto()
                }
            )
            .build()
    }
}
