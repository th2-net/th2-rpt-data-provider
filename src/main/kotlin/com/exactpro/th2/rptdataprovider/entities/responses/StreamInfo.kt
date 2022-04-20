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
import com.exactpro.cradle.testevents.StoredTestEventId
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
    val streamName: StreamName,
    val hasStarted: Boolean,
    val hasEnded: Boolean,
    val lastId: StoredMessageId? = null
) {
    companion object {
        val logger = KotlinLogging.logger { }
    }

    constructor(lastId: MessageID, messageStream: StreamName, hasStarted: Boolean, hasEnded: Boolean) : this(
        lastId = StoredMessageId(
            lastId.connectionId.sessionAlias,
            lastId.direction.toCradleDirection(),
            lastId.sequence
        ),
        streamName = messageStream,
        hasStarted = hasStarted,
        hasEnded = hasEnded
    )

    fun convertToProto(): MessageStreamPointer {
        return MessageStreamPointer.newBuilder()
            .setMessageStream(
                MessageStream.newBuilder()
                    .setDirection(cradleDirectionToGrpc(streamName.direction))
                    .setName(streamName.name)
            )
            .setHasStarted(hasStarted)
            .setHasEnded(hasEnded)
            .setLastId(
                lastId?.let {
                    logger.trace { "stream $streamName - lastElement is ${it.index}" }
                    it.convertToProto()

                } ?: let {
                    // set sequence to a negative value if stream has not started to avoid mistaking it for the default value
                    // FIXME change interface to make that case explicit
                    logger.trace { "lastElement is null - StreamInfo should be build as if the stream $streamName has no messages" }

                    MessageID
                        .newBuilder()
                        .setSequence(-1)
                        .setDirection(cradleDirectionToGrpc(streamName.direction))
                        .setConnectionId(ConnectionID.newBuilder().setSessionAlias(streamName.name).build())
                        .build()
                }
            )
            .build()
    }
}
