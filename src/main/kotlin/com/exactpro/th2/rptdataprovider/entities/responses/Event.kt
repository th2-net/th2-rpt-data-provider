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
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.ByteString
import java.time.Instant

data class Event(
    val type: String = "event",
    val eventId: String,
    val batchId: String?,
    val isBatched: Boolean,
    val eventName: String,
    val eventType: String?,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: String?,
    val successful: Boolean,
    val attachedMessageIds: Set<String>,

    @JsonRawValue
    val body: String?
) {

    private fun convertMessageIdToProto(attachedMessageIds: Set<String>): List<MessageID> {
        return attachedMessageIds.map { id ->
            StoredMessageId.fromString(id).let {
                MessageID.newBuilder()
                    .setConnectionId(ConnectionID.newBuilder().setSessionAlias(it.streamName))
                    .setDirection(cradleDirectionToGrpc(it.direction))
                    .setSequence(it.index)
                    .build()
            }
        }
    }

    fun convertToGrpcEventData(): EventResponse {
        return EventResponse.newBuilder()
            .setEventId(EventID.newBuilder().setId(eventId))
            .setIsBatched(isBatched)
            .setEventName(eventName)
            .setStartTimestamp(startTimestamp.toTimestamp())
            .setStatus(if (successful) SUCCESS else FAILED)
            .addAllAttachedMessageId(convertMessageIdToProto(attachedMessageIds))
            .also { builder ->
                batchId?.let { builder.setBatchId(EventID.newBuilder().setId(it)) }
                eventType?.let { builder.setEventType(it) }
                endTimestamp?.let { builder.setEndTimestamp(it.toTimestamp()) }
                parentEventId?.let { builder.setParentEventId(EventID.newBuilder().setId(it)) }
                body?.let { builder.setBody(ByteString.copyFrom(it.toByteArray())) }
            }.build()
    }

    constructor(
        stored: StoredTestEventWithContent,
        eventId: String,
        messages: Set<String>,
        batchId: String?,
        parentEventId: String?,
        body: String?
    ) : this(
        batchId = batchId,
        isBatched = batchId != null,
        eventId = eventId,
        eventName = stored.name ?: "",
        eventType = stored.type ?: "",
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = parentEventId,
        successful = stored.isSuccess,
        attachedMessageIds = messages,
        body = body
    )
}
