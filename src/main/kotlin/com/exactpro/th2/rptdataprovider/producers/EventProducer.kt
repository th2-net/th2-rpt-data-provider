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

import com.exactpro.cradle.testevents.*
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType.NEED_ATTACHED_MESSAGES
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType.NEED_BODY
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.services.cradle.CradleEventNotFoundException
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.tryToGetTestEvents
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.util.*

class EventProducer(private val cradle: CradleService, private val mapper: ObjectMapper) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private suspend fun fromBatchIdsProcessed(batch: List<BaseEventEntity>): List<BaseEventEntity> {
        return batch.map { setBody(it) }
    }

    private suspend fun fromBatchIds(batch: StoredTestEventBatch?, ids: List<ProviderEventId>): List<TestEventSingle?> {
        return ids.map {
            val storedEvent =
                batch?.getTestEvent(it.eventId) ?: cradle.getEventSuspend(it.eventId)?.asSingle()

            if (storedEvent == null) {
                logger.error { "unable to find event '$it'. It is not a valid id" }
                null
            } else {
                storedEvent
            }
        }
    }

    suspend fun fromId(id: ProviderEventId): BaseEventEntity {
        val batch = id.batchId?.let { cradle.getEventSuspend(it)?.asBatch() }

        if (id.batchId != null && batch == null) {
            logger.error { "unable to find batch with id '${id.batchId}' referenced in event '${id.eventId}'- this is a bug" }
        }

        val storedEvent = batch?.getTestEvent(id.eventId) ?: cradle.getEventSuspend(id.eventId)?.asSingle()

        if (storedEvent == null) {
            logger.error { "unable to find event '${id.eventId}'" }
            throw CradleEventNotFoundException("${id.eventId} is not a valid id")
        }

        return fromStoredEvent(storedEvent, batch).let {
            setBody(it)
        }.let {
            setAttachedMessage(listOf(it)).first()
        }
    }

    suspend fun fromIds(ids: List<ProviderEventId>): List<BaseEventEntity> {
        val batchOrSingle = ids.groupBy { it.batchId }
        val singleEvents = batchOrSingle[null]
            ?.mapNotNull { cradle.getEventSuspend(it.eventId) }
            ?.map { setBody(fromStoredEvent(it.asSingle(), null)) }
            ?: emptyList()

        val batchedEvents = batchOrSingle.keys
            .filterNotNull()
            .toSet()
            .let { batchIds ->
                val filters = batchIds.map { TestEventFilter(it.bookId, it.scope) }

                val storedTestEvents = emptyList<StoredTestEvent>().toMutableList()
                filters.map { cradle.getCompletedEventSuspend(it).forEach { storedTestEvents.add(it) } }
                val batches = storedTestEvents.map { it.asBatch() }.associateBy { it.id }

                batchIds.flatMap { batchId ->
                    val storedEventBatch = batches[batchId]
                    val storedEventIds = batchOrSingle.getValue(batchId)
                    fromBatchIds(storedEventBatch, storedEventIds).mapNotNull { storedEvent ->
                        storedEvent?.let { setBody(fromStoredEvent(it, storedEventBatch)) }
                    }
                }
            }

        return setAttachedMessage(singleEvents + batchedEvents)
    }


    suspend fun fromBatchIdsProcessed(
        eventsMetadata: List<BaseEventEntity>,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        return eventsMetadata.let { events ->
            if (!request.metadataOnly || request.filterPredicate.getSpecialTypes().contains(NEED_BODY)) {
                events.map { setBody(it) }
            } else {
                events
            }
        }.let {
            if (!request.metadataOnly && request.attachedMessages
                || request.filterPredicate.getSpecialTypes().contains(NEED_ATTACHED_MESSAGES)
            ) {
                setAttachedMessage(it)
            } else {
                it
            }
        }
    }

    suspend fun fromSingleEventsProcessed(
        eventsMetadata: BaseEventEntity,
        request: SseEventSearchRequest
    ): BaseEventEntity {
        return eventsMetadata.let { events ->
            if (!request.metadataOnly || request.filterPredicate.getSpecialTypes().contains(NEED_BODY)) {
                setBody(eventsMetadata)
            } else {
                events
            }
        }.let {
            if (!request.metadataOnly && request.attachedMessages
                || request.filterPredicate.getSpecialTypes().contains(NEED_ATTACHED_MESSAGES)
            ) {
                setAttachedMessage(listOf(it)).first()
            } else {
                it
            }
        }
    }

    fun fromStoredEvent(storedEvent: TestEventSingle, batch: StoredTestEventBatch?): BaseEventEntity {
        return BaseEventEntity(
            storedEvent,
            ProviderEventId(batch?.id, storedEvent.id),
            batch?.id,
            storedEvent.parentId?.let { parentId ->
                if (batch?.tryToGetTestEvents()?.firstOrNull { it.id == parentId } != null) {
                    ProviderEventId(batch?.id, parentId)
                } else {
                    ProviderEventId(null, parentId)
                }
            }
        )
    }


    private suspend fun setAttachedMessage(
        baseEvents: List<BaseEventEntity>
    ): List<BaseEventEntity> {
        return coroutineScope {
            baseEvents.map { event ->
                async {
                    event.apply {
                        attachedMessageIds = event.id.eventId.let {
                            try {
                                cradle.getMessageIdsSuspend(it).map(Any::toString).toSet()
                            } catch (e: Exception) {
                                KotlinLogging.logger { }
                                    .error(e) { "unable to get messages attached to event (id=${event.id})" }

                                Collections.emptySet<String>()
                            }
                        }
                    }
                }
            }.awaitAll()
        }
    }

    private suspend fun setBody(baseEvent: BaseEventEntity): BaseEventEntity {
        return baseEvent.apply {
            body = this.rawValue?.content?.let {
                try {
                    val data = String(it).takeUnless(String::isEmpty) ?: "{}"
                    mapper.readTree(data)
                    data
                } catch (e: Exception) {
                    KotlinLogging.logger { }
                        .warn(e) { "unable to write event content (id=${this.id}) to 'body' property - invalid data" }

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
