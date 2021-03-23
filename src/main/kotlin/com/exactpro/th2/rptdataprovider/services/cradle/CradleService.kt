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


package com.exactpro.th2.rptdataprovider.services.cradle

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventMetadata
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.rptdataprovider.convertToString
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.logTime
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.time.Instant
import kotlin.coroutines.coroutineContext

class CradleService(configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger {}
    }

    private val cradleManager: CradleManager = configuration.cradleManager

    private val storage = cradleManager.storage
    private val linker = cradleManager.storage.testEventsMessagesLinker

    suspend fun getMessagesSuspend(filter: StoredMessageFilter): Iterable<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logTime("getMessages (filter=${filter.convertToString()})") {
                storage.getMessagesAsync(filter).await()
            }
        } ?: listOf()
    }

    suspend fun getProcessedMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logTime("getProcessedMessage (id=$id)") {
                storage.getProcessedMessageAsync(id).await()
            }
        }
    }

    suspend fun getMessageSuspend(id: StoredMessageId): StoredMessage? {
        return withContext(Dispatchers.IO) {
            logTime("getMessage (id=$id)") {
                storage.getMessageAsync(id).await()
            }
        }
    }

    suspend fun getEventsSuspend(from: Instant, to: Instant): Iterable<StoredTestEventMetadata> {
        return withContext(coroutineContext) {
            logTime("Get events from: $from to: $to") {
                storage.getTestEventsAsync(from, to).await()
            }
        } ?: listOf()

    }

    suspend fun getEventsSuspend(
        parentId: StoredTestEventId,
        from: Instant,
        to: Instant
    ): Iterable<StoredTestEventMetadata> {
        return withContext(coroutineContext) {
            logTime("Get events parent: $parentId from: $from to: $to") {
                storage.getTestEventsAsync(parentId, from, to).await()
            }
        } ?: listOf()

    }

    suspend fun getEventSuspend(id: StoredTestEventId): StoredTestEventWrapper? {
        return withContext(coroutineContext) {
            logTime("getTestEvent (id=$id)") {
                storage.getTestEventAsync(id).await()
            }
        }
    }

    suspend fun getFirstMessageIdSuspend(
        timestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation
    ): StoredMessageId? {
        return withContext(Dispatchers.IO) {
            logTime(("getFirstMessageId (timestamp=$timestamp stream=$stream direction=${direction.label} )")) {
                storage.getNearestMessageId(stream, direction, timestamp, timelineDirection)
            }
        }
    }

    suspend fun getMessageBatchSuspend(id: StoredMessageId): Collection<StoredMessage> {
        return withContext(Dispatchers.IO) {
            logTime("getMessageBatchByMessageId (id=$id)") {
                storage.getMessageBatchAsync(id).await()
            }
        } ?: emptyList()
    }

    suspend fun getEventIdsSuspend(id: StoredMessageId): Collection<StoredTestEventId> {
        return withContext(Dispatchers.IO) {
            logTime("getTestEventIdsByMessageId (id=$id)") {
                linker.getTestEventIdsByMessageIdAsync(id).await()
            }
        } ?: emptyList()
    }

    suspend fun getMessageIdsSuspend(id: StoredTestEventId): Collection<StoredMessageId> {
        return withContext(Dispatchers.IO) {
            logTime("getMessageIdsByTestEventId (id=$id)") {
                linker.getMessageIdsByTestEventIdAsync(id).await()
            }
        } ?: emptyList()
    }

    suspend fun getMessageStreams(): Collection<String> {
        return withContext(Dispatchers.IO) {
            logTime("getStreams") {
                storage.streams
            }
        } ?: emptyList()
    }
}
