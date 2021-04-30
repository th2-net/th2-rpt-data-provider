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

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.fasterxml.jackson.annotation.JsonRawValue
import java.time.Instant


data class BaseEventEntity(
    val type: String = "event",
    val id: ProviderEventId,
    val batchId: StoredTestEventId?,
    val isBatched: Boolean,
    val eventName: String,
    val eventType: String,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: ProviderEventId?,
    val successful: Boolean
) {

    var attachedMessageIds: Set<String> = emptySet()

    @JsonRawValue
    var body: String? = null

    constructor(
        stored: StoredTestEventMetadata,
        eventId: ProviderEventId,
        batchId: StoredTestEventId?,
        parentEventId: ProviderEventId?
    ) : this(
        batchId = batchId,
        isBatched = batchId != null,
        id = eventId,
        eventName = stored.name ?: "",
        eventType = stored.type ?: "",
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = parentEventId,
        successful = stored.isSuccess
    )

    fun convertToEventTreeNode(): EventTreeNode {
        return EventTreeNode(
            eventName = eventName,
            eventType = eventType,
            successful = successful,
            startTimestamp = startTimestamp,
            parentEventId = parentEventId,
            id = id
        )
    }

    fun convertToEvent(): Event {
        return Event(
            batchId = batchId?.toString(),
            isBatched = batchId != null,
            eventId = id.toString(),
            eventName = eventName,
            eventType = eventType,
            startTimestamp = startTimestamp,
            endTimestamp = endTimestamp,
            parentEventId = parentEventId?.toString(),
            successful = successful,
            attachedMessageIds = attachedMessageIds,
            body = body
        )
    }
}
