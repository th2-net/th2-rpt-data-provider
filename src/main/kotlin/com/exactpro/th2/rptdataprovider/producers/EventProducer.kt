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

package com.exactpro.th2.rptdataprovider.producers

import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventSingle
import com.exactpro.cradle.testevents.TestEventSingle
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType.NEED_ATTACHED_MESSAGES
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType.NEED_BODY
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.tryToGetTestEvents
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging

class EventProducer(private val cradle: CradleService, private val mapper: ObjectMapper) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private suspend fun fromSingle(batch: List<ProviderEventId>): List<TestEventSingle?> {
        return batch
            .map { it.eventId }
            .distinct()
            .map { it to cradle.getEventSuspend(it)?.asSingle() }
            .map { (eventId, storedEvent) ->
                if (storedEvent == null) {
                    logger.error { "unable to find event '$eventId' - this id is invalid or the event is missing" }
                    null
                } else {
                    storedEvent
                }
            }
    }

    private suspend fun fromBatchIds(
        batchId: StoredTestEventId,
        ids: List<ProviderEventId>
    ): List<TestEventSingle?> {
        val batchedEvents = cradle.getEventSuspend(batchId).let {
            if (it == null) {
                logger.error { "unable to find batch '$batchId' - this id is invalid or the batch is missing" }
                null
            } else {
                it.asBatch().testEvents
            }
        }?.associate { it.id to it } ?: emptyMap()

        return ids.map { it.eventId }.map { eventId ->
            if (batchedEvents.contains(eventId)) {
                logger.error { "unable to find event '$eventId' - this id is invalid or the event is missing" }
                null
            } else {
                batchedEvents[eventId]
            }
        }
    }

    suspend fun fromId(id: ProviderEventId): BaseEventEntity {
        val batch = id.batchId?.let { cradle.getEventSuspend(it)?.asBatch() }

        if (id.batchId != null && batch == null) {
            logger.error { "unable to find batch with id '${id.batchId}' referenced in event '${id.eventId}'- this is a rpt-data-provider bug" }
        }

        val storedEvent = batch?.getTestEvent(id.eventId) ?: cradle.getEventSuspend(id.eventId)?.asSingle()

        if (storedEvent == null) {
            logger.error { "unable to find event '${id.eventId}' - this id is invalid or the event is missing" }
            throw CradleEventNotFoundException(id.eventId.toString())
        }

        return fromStoredEvent(storedEvent, batch).let {
            setBody(storedEvent, it).apply {
                it.attachedMessageIds = storedEvent.messages?.map(Any::toString)?.toSet() ?: emptySet()
            }
        }
    }

    suspend fun fromIds(ids: List<ProviderEventId>): List<BaseEventEntity> {
        return ids.groupBy { it.batchId }.flatMap { (batchId, events) ->
            if (batchId == null) {
                fromSingle(events)
            } else {
                fromBatchIds(batchId, events)
            }
        }
            .filterNotNull()
            .map {
                setBody(it, fromStoredEvent(it, null)).apply {
                    attachedMessageIds = it.messages?.map(Any::toString)?.toSet() ?: emptySet()
                }
            }
            .toList()
    }

    fun fromEventsProcessed(
        events: List<Pair<TestEventSingle, BaseEventEntity>>,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {

        return events.let {
            if (!request.metadataOnly || request.filterPredicate.getSpecialTypes().contains(NEED_BODY)) {
                it.map { (content, event) ->
                    content to setBody(content, event)
                }
            } else {
                it
            }
        }.let {
            if (request.attachedMessages
                || request.filterPredicate.getSpecialTypes().contains(NEED_ATTACHED_MESSAGES)
            ) {
                it.map { (content, event) ->
                    event.apply {
                        attachedMessageIds = content.messages?.map(Any::toString)?.toSet() ?: emptySet()
                    }
                }
            } else {
                it.map { (_, event) -> event }
            }
        }
    }


    fun fromStoredEvent(
        storedEvent: TestEventSingle,
        batch: StoredTestEventBatch?
    ): BaseEventEntity {
        return BaseEventEntity(
            storedEvent,
            ProviderEventId(batch?.id, storedEvent.id),
            batch?.id,
            storedEvent.parentId?.let { parentId ->
                if (batch?.getTestEvent(parentId) != null) {
                    ProviderEventId(batch?.id, parentId)
                } else {
                    ProviderEventId(null, parentId)
                }
            }
        )
    }


    private fun setBody(
        storedEvent: TestEventSingle,
        baseEvent: BaseEventEntity
    ): BaseEventEntity {
        return baseEvent.apply {
            body = storedEvent.content.let {
                try {
                    val data = String(it).takeUnless(String::isEmpty) ?: "{}"

                    //FIXME: Delete later it's slowdown

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
        }
    }
}