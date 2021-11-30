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
import javax.xml.stream.EventFilter

class EventProducer(private val cradle: CradleService, private val mapper: ObjectMapper) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }


    private suspend fun fromBatchIdsProcessed(
        batch: List<Pair<StoredTestEventId, List<BaseEventEntity>>>
    ): List<BaseEventEntity> {
        val requestIds = batch.map { it.first }.toSet()
        val testEventFilters = requestIds.map { TestEventFilter(it.bookId,it.scope) }
        val storedTestEventBatch = emptyList<StoredTestEvent>().toMutableList()
        testEventFilters.map { cradle.getCompletedEventSuspend(it).forEach { event -> storedTestEventBatch.add(event) } }
        val completedEvents = storedTestEventBatch.map { it.asBatch() }.associateBy { it.id }

        return coroutineScope {
            batch.flatMap {
                val storedEventBatch = completedEvents[it.first]
                fromBatch(storedEventBatch, it.second)
            }
        }
    }

    private suspend fun fromSingleIdsProcessed(batch: List<BaseEventEntity>): List<BaseEventEntity> {
        return coroutineScope {
            batch.zip(fromSingle(batch.map { it.id })).mapNotNull { (baseEventEntity, event) ->
                event?.let { setBody(it, baseEventEntity) }
            }
        }
    }

    private suspend fun fromSingle(batch: List<ProviderEventId>): List<StoredTestEventSingle?> {
        val requestIds = batch.map { it.eventId }.toSet()
        val testEventFilters = requestIds.map { TestEventFilter(it.bookId,it.scope) }
        val storedTestEvents = emptyList<StoredTestEvent>().toMutableList()
        testEventFilters.map { cradle.getCompletedEventSuspend(it).forEach { event -> storedTestEvents.add(event) } }
        val completedEvents = storedTestEvents.map { it.asSingle() }.associateBy { it.id }

        return coroutineScope {
            batch.map {
                val storedEvent = completedEvents[it.eventId]
                if (storedEvent == null) {
                    logger.error { "unable to find event '$it'. It is not a valid id" }
                    null
                } else {
                    storedEvent
                }
            }
        }
    }

    private suspend fun fromBatch(batch: StoredTestEventBatch?, ids: List<BaseEventEntity>): List<BaseEventEntity> {
        return coroutineScope {
            ids.zip(fromBatchIds(batch, ids.map { it.id })).mapNotNull { (baseEventEntity, event) ->
                event?.let { setBody(it, baseEventEntity) }
            }
        }
    }

    private suspend fun fromBatchIds(
        batch: StoredTestEventBatch?,
        ids: List<ProviderEventId>
    ): List<TestEventSingle?> {
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
            setBody(storedEvent, it)
        }.let {
            setAttachedMessage(listOf(it)).first()
        }
    }

    suspend fun fromIds(ids: List<ProviderEventId>): List<BaseEventEntity> {
        val batchOrSingle = ids.groupBy { it.batchId }
        val singleEvents = batchOrSingle[null]
            ?.let { fromSingle(it).filterNotNull() }
            ?.map { setBody(it, fromStoredEvent(it, null)) }
            ?: emptyList()

        val batchedEvents = batchOrSingle.keys
            .filterNotNull()
            .map { StoredTestEventId(it.bookId,it.scope,it.startTimestamp,it.id) }
            .toSet()
            .let { batchIds ->
                val filters = batchIds.map { TestEventFilter(it.bookId,it.scope) }

                val storedTestEvents = emptyList<StoredTestEvent>().toMutableList()
                filters.map { cradle.getCompletedEventSuspend(it).forEach { storedTestEvents.add(it) } }
                val batches = storedTestEvents.map { it.asBatch() }.associateBy { it.id }

                batchIds.flatMap { batchId ->
                    val storedEventBatch = batches[batchId]
                    val storedEventIds = batchOrSingle.getValue(batchId)
                    fromBatchIds(storedEventBatch, storedEventIds).mapNotNull { storedEvent ->
                        storedEvent?.let { setBody(it, fromStoredEvent(it, storedEventBatch)) }
                    }
                }
            }

        return setAttachedMessage(singleEvents + batchedEvents)
    }


    suspend fun fromBatchIdsProcessed(
        eventsMetadata: List<Pair<StoredTestEventId, List<BaseEventEntity>>>,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        return eventsMetadata.let { events ->
            if (!request.metadataOnly || request.filterPredicate.getSpecialTypes().contains(NEED_BODY)) {
                fromBatchIdsProcessed(events)
            } else {
                events.flatMap { it.second }
            }
        }.let {
            if (!request.metadataOnly && request.attachedMessages
                || request.filterPredicate.getSpecialTypes().contains(NEED_ATTACHED_MESSAGES)) {
                setAttachedMessage(it)
            } else {
                it
            }
        }
    }


    suspend fun fromSingleEventsProcessed(
        eventsMetadata: List<BaseEventEntity>,
        request: SseEventSearchRequest
    ): List<BaseEventEntity> {
        return eventsMetadata.let {
            if (!request.metadataOnly || request.filterPredicate.getSpecialTypes().contains(NEED_BODY)) {
                fromSingleIdsProcessed(it)
            } else {
                it
            }
        }.let {
            if (!request.metadataOnly && request.attachedMessages
                || request.filterPredicate.getSpecialTypes().contains(NEED_ATTACHED_MESSAGES)) {
                setAttachedMessage(it)
            } else {
                it
            }
        }
    }


    fun fromEventMetadata(
        storedEvent: TestEventSingle,
        batch: StoredTestEventBatch?
    ): BaseEventEntity {
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


    private suspend fun setBody(
        storedEvent: TestEventSingle,
        baseEvent: BaseEventEntity
    ): BaseEventEntity {
        return baseEvent.apply {
            body = storedEvent.content.let {
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
        }
    }
}
