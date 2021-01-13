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

package com.exactpro.th2.rptdataprovider.entities.sse

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.fasterxml.jackson.databind.ObjectMapper
import java.security.Timestamp

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE;

    override fun toString(): String {
        return super.toString().toLowerCase()
    }
}

data class LastScannedObjectInfo(val id: String, val timestamp: Long)

/**
 * The data class representing a SSE Event that will be sent to the client.
 */

data class SseEvent(val data: String = "empty data", val event: EventType? = null, val metadata: String? = null) {
    companion object {
        suspend fun build(jacksonMapper: ObjectMapper, event: EventTreeNode): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(event),
                EventType.EVENT,
                jacksonMapper.asStringSuspend(
                    LastScannedObjectInfo(
                        event.eventId,
                        event.startTimestamp.toEpochMilli()
                    )
                )
            )
        }

        suspend fun build(jacksonMapper: ObjectMapper, message: Message): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(message),
                EventType.MESSAGE,
                jacksonMapper.asStringSuspend(
                    LastScannedObjectInfo(
                        message.id.toString(),
                        message.timestamp.toEpochMilli()
                    )
                )
            )
        }
    }
}


