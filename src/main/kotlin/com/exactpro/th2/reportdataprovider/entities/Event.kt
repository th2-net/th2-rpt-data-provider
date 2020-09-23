/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider.entities

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.jacksonMapper
import com.fasterxml.jackson.annotation.JsonRawValue
import mu.KotlinLogging
import java.time.Instant
import java.util.*

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
    constructor(stored: StoredTestEventWithContent, cradleManager: CradleManager)
            : this(stored, cradleManager, null)

    constructor(
        stored: StoredTestEventWithContent,
        cradleManager: CradleManager,
        batchId: String?
    ) : this(
        batchId = batchId,
        isBatched = batchId != null,
        eventId = stored.id.toString(),
        eventName = stored.name ?: "unknown",
        eventType = stored.type ?: "unknown",
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = stored.parentId?.toString(),
        successful = stored.isSuccess,

        attachedMessageIds = stored.id.let {
            try {
                cradleManager.storage?.testEventsMessagesLinker
                    ?.getMessageIdsByTestEventId(it)?.map(Any::toString)?.toSet().orEmpty()
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error(e) { "unable to get messages attached to event (id=${stored.id})" }

                Collections.emptySet<String>()
            }
        },

        body = stored.content.let {
            try {
                val data = String(it).takeUnless(String::isEmpty) ?: "{}"
                jacksonMapper.readTree(data)
                data
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .warn(e) { "unable to write event content (id=${stored.id}) to 'body' property - invalid data" }

                jacksonMapper.writeValueAsString(listOf(
                    object {
                        val type = "message"
                        val data = "Content of this event is an invalid object"
                    },
                    object {
                        val type = "message"
                        val data = "raw event body: \n${String(it)}"
                    },
                    object {
                        val type = "message"
                        val data = "error: \n$e"
                    }
                ))
            }
        }
    )
}
