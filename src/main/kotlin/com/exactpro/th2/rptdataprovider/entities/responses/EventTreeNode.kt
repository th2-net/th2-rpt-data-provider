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

import com.exactpro.cradle.testevents.BatchedStoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.EventMetadata
import com.exactpro.th2.rptdataprovider.entities.exceptions.ParseEventTreeNodeException
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.fasterxml.jackson.annotation.JsonIgnore
import mu.KotlinLogging
import java.time.Instant

data class EventTreeNode(
    val type: String = "eventTreeNode",

    val eventName: String,
    val eventType: String,
    val successful: Boolean,
    val startTimestamp: Instant,

    @JsonIgnore
    var parentEventId: ProviderEventId?,

    @JsonIgnore
    val id: ProviderEventId
) {

    val eventId: String
        get() = id.toString()

    val parentId: String?
        get() = parentEventId?.toString()

    companion object {
        private const val error = "field is null in both batched and non-batched event metadata"
        private val logger = KotlinLogging.logger { }
    }

    constructor(
        batch: StoredTestEventBatch?,
        nonBatchedEvent: StoredTestEvent?,
        batchedEvent: BatchedStoredTestEvent?
    ) : this(
        batch = batch,
        batchedEvent = nonBatchedEvent,
        nonBatchedEvent = batchedEvent,
        providerEventId = ProviderEventId(
            batch?.id, nonBatchedEvent?.id ?: batchedEvent?.id ?: throw ParseEventTreeNodeException(error)
        ),

        parentEventId = (nonBatchedEvent?.parentId ?: batchedEvent?.parentId)?.let {
            if (batch?.getTestEvent(it) != null) {
                ProviderEventId(batch?.id, it)
            } else {
                ProviderEventId(null, it)
            }
        }
    )

    constructor(
        batch: StoredTestEventBatch?,
        batchedEvent: StoredTestEvent?,
        nonBatchedEvent: BatchedStoredTestEvent?,
        providerEventId: ProviderEventId,
        parentEventId: ProviderEventId?
    ) : this(
        id = providerEventId,
        parentEventId = parentEventId,

        eventName = batchedEvent?.name ?: nonBatchedEvent?.name ?: "",
        eventType = batchedEvent?.type ?: nonBatchedEvent?.type ?: "",

        successful = batchedEvent?.isSuccess ?: nonBatchedEvent?.isSuccess
        ?: throw ParseEventTreeNodeException(error),

        startTimestamp = batchedEvent?.startTimestamp ?: nonBatchedEvent?.startTimestamp
        ?: throw ParseEventTreeNodeException(error)
    )

    constructor(batch: StoredTestEventBatch?, event: StoredTestEvent) : this(
        batch, event, null
    )

    constructor(batch: StoredTestEventBatch?, event: BatchedStoredTestEvent) : this(
        batch, null, event
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventTreeNode

        if (id.eventId != other.id.eventId) return false

        return true
    }

    override fun hashCode(): Int {
        return id.eventId.hashCode()
    }


    fun convertToGrpcEventMetadata(): EventMetadata {
        return EventMetadata.newBuilder()
            .setEventId(EventID.newBuilder().setId(eventId))
            .setEventName(eventName)
            .setEventType(eventType)
            .setStartTimestamp(startTimestamp.toTimestamp())
            .setSuccessful(if (successful) EventStatus.SUCCESS else EventStatus.FAILED)
            .let { builder ->
                parentEventId?.let { builder.setParentEventId(EventID.newBuilder().setId(parentId)) }
                builder
            }.build()
    }

}