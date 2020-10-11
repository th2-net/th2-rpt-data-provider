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

/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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

/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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

/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.reportdataprovider.producers

import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.reportdataprovider.entities.responses.Event
import com.exactpro.th2.reportdataprovider.services.CradleService
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import java.util.*

class EventProducer(private val cradle: CradleService, private val mapper: ObjectMapper) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    suspend fun fromId(id: ProviderEventId): Event {
        val batch = id.batchId?.let { cradle.getEventSuspend(it)?.asBatch() }

        if (id.batchId != null && batch == null) {
            logger.error { "unable to find batch with id '${id.batchId}' referenced in event '${id.eventId}'- this is a bug" }
        }

        val storedEvent = batch?.getTestEvent(id.eventId) ?: cradle.getEventSuspend(id.eventId)?.asSingle()

        if (storedEvent == null) {
            logger.error { "unable to find event '${id.eventId}'" }
            throw IllegalArgumentException("${id.eventId} is not a valid id")
        }

        return fromStoredEvent(storedEvent, batch)
    }

    private suspend fun fromStoredEvent(storedEvent: StoredTestEventWithContent, batch: StoredTestEventBatch?): Event {
        return Event(
            storedEvent,
            storedEvent.id.let {
                try {
                    cradle.getMessageIdsSuspend(it).map(Any::toString).toSet()
                } catch (e: Exception) {
                    KotlinLogging.logger { }
                        .error(e) { "unable to get messages attached to event (id=${storedEvent.id})" }

                    Collections.emptySet<String>()
                }
            },
            batch?.id?.toString(),
            storedEvent.content.let {
                try {
                    val data = String(it).takeUnless(String::isEmpty) ?: "{}"
                    mapper.readTree(data)
                    data
                } catch (e: Exception) {
                    KotlinLogging.logger { }
                        .warn(e) { "unable to write event content (id=${storedEvent.id}) to 'body' property - invalid data" }

                    mapper.writeValueAsString(listOf(
                        object {
                            val type = "message"
                            val data = "Error - content of this event is an invalid object"
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
}
