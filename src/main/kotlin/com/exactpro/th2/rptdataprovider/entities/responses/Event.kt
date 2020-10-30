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

import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.fasterxml.jackson.annotation.JsonRawValue
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
    val attachedMessageIds: Set<String>?,

    @JsonRawValue
    val body: String?
) {
    constructor(
        stored: StoredTestEventWithContent,
        messages: Set<String>,
        batchId: String?,
        body: String?
    ) : this(
        batchId = batchId,
        isBatched = batchId != null,
        eventId = stored.id.toString(),
        eventName = stored.name ?: "",
        eventType = stored.type ?: "",
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = stored.parentId?.toString(),
        successful = stored.isSuccess,
        attachedMessageIds = messages,
        body = body
    )
}
