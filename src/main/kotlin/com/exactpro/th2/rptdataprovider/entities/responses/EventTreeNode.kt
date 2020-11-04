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

import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventBatchMetadata
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Instant

data class EventTreeNode(
    val type: String = "eventTreeNode",

    val eventName: String,
    val eventType: String,
    val successful: Boolean,
    val startTimestamp: Instant,
    val childList: MutableSet<EventTreeNode>,
    var filtered: Boolean,

    @JsonIgnore
    var parentEventId: ProviderEventId?,

    @JsonIgnore
    val id: ProviderEventId,

    @JsonIgnore
    val batch: StoredTestEventBatchMetadata?,

    @JsonIgnore
    val batchedEvent: StoredTestEventMetadata?,

    @JsonIgnore
    val nonBatchedEvent: BatchedStoredTestEventMetadata?
) {

    val eventId: String
        get() = id.toString()

    val parentId: String
        get() = parentEventId.toString()

    companion object {
        const val error = "field is null in both batched and non-batched event metadata"
    }

    constructor(
        batch: StoredTestEventBatchMetadata?,
        nonBatchedEvent: StoredTestEventMetadata?,
        batchedEvent: BatchedStoredTestEventMetadata?,
        filtered: Boolean
    ) : this(
        batch = batch,
        batchedEvent = nonBatchedEvent,
        nonBatchedEvent = batchedEvent,
        filtered = filtered,

        providerEventId = ProviderEventId(
            batch?.id, nonBatchedEvent?.id ?: batchedEvent?.id ?: throw IllegalArgumentException(error)
        ),

        parentEventId = (nonBatchedEvent?.parentId ?: batchedEvent?.parentId)?.let { ProviderEventId(batch?.id, it) }
    )

    constructor(
        batch: StoredTestEventBatchMetadata?,
        batchedEvent: StoredTestEventMetadata?,
        nonBatchedEvent: BatchedStoredTestEventMetadata?,
        filtered: Boolean,
        providerEventId: ProviderEventId,
        parentEventId: ProviderEventId?
    ) : this(
        id = providerEventId,
        parentEventId = parentEventId,

        eventName = batchedEvent?.name ?: nonBatchedEvent?.name ?: "",
        eventType = batchedEvent?.type ?: nonBatchedEvent?.type ?: "",

        successful = batchedEvent?.isSuccess ?: nonBatchedEvent?.isSuccess
        ?: throw IllegalArgumentException(error),

        startTimestamp = batchedEvent?.startTimestamp ?: nonBatchedEvent?.startTimestamp
        ?: throw IllegalArgumentException(error),

        childList = mutableSetOf<EventTreeNode>(),
        filtered = filtered,
        batch = batch,
        batchedEvent = batchedEvent,
        nonBatchedEvent = nonBatchedEvent
    )

    constructor(batch: StoredTestEventBatchMetadata?, event: StoredTestEventMetadata, filtered: Boolean) : this(
        batch, event, null, filtered
    )

    constructor(batch: StoredTestEventBatchMetadata?, event: BatchedStoredTestEventMetadata, filtered: Boolean) : this(
        batch, null, event, filtered
    )

    constructor(batch: StoredTestEventBatchMetadata?, event: StoredTestEventMetadata) : this(
        batch, event, null, true
    )

    constructor(batch: StoredTestEventBatchMetadata?, event: BatchedStoredTestEventMetadata) : this(
        batch, null, event, true
    )

    fun addChild(child: EventTreeNode) {
        childList.add(child)
    }

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
}
