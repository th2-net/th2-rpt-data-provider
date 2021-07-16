/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset

class StreamBasketProducer(
    private val context: Context,
    private val coroutineScope: CoroutineScope
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private fun nextDay(timestamp: Instant, timelineDirection: TimeRelation): Instant {
        val utcTimestamp = timestamp.atOffset(ZoneOffset.UTC)
        return if (timelineDirection == TimeRelation.AFTER) {
            utcTimestamp.plusDays(1)
                .with(LocalTime.of(0, 0, 0, 0))
        } else {
            utcTimestamp.with(LocalTime.of(0, 0, 0, 0))
                .minusNanos(1)
        }.toInstant()
    }

    private suspend fun chooseStartTimestamp(messageId: StoredMessageId?, startTimestamp: Instant?): Instant {
        return messageId?.let { context.cradleService.getMessageSuspend(it)?.timestamp }
            ?: startTimestamp
            ?: Instant.now()
    }


    private suspend fun getNearestMessage(
        messageBatch: Collection<StoredMessage>,
        timelineDirection: TimeRelation,
        timestamp: Instant
    ): StoredMessage? {
        if (messageBatch.isEmpty()) return null
        return if (timelineDirection == TimeRelation.AFTER)
            messageBatch.find { it.timestamp.isAfter(timestamp) } ?: messageBatch.lastOrNull()
        else
            messageBatch.findLast { it.timestamp.isBefore(timestamp) } ?: messageBatch.firstOrNull()

    }

    private suspend fun getFirstMessageCurrentDay(
        timestamp: Instant,
        stream: String,
        direction: Direction
    ): StoredMessageId? {
        for (timeRelation in listOf(TimeRelation.BEFORE, TimeRelation.AFTER)) {
            context.cradleService.getFirstMessageIdSuspend(timestamp, stream, direction, timeRelation)?.let {
                return it
            }
        }
        return null
    }

    private suspend fun getFirstMessageIdDifferentDays(
        startTimestamp: Instant,
        stream: String,
        direction: Direction,
        timelineDirection: TimeRelation,
        daysInterval: Int = 1000
    ): StoredMessageId? {
        var daysChecking = daysInterval
        var isCurrentDay = true
        var timestamp = startTimestamp
        var messageId: StoredMessageId? = null
        while (messageId == null && daysChecking >= 0) {
            messageId =
                if (isCurrentDay) {
                    getFirstMessageCurrentDay(timestamp, stream, direction)
                } else {
                    context.cradleService.getFirstMessageIdSuspend(timestamp, stream, direction, timelineDirection)
                }
            daysChecking -= 1
            isCurrentDay = false
            timestamp = nextDay(timestamp, timelineDirection)
        }
        return messageId
    }

    private suspend fun initStreamsInfoFromIds(request: SseMessageSearchRequest): List<StreamBasket> {
        return coroutineScope {
            mutableListOf<StreamBasket>().apply {
                request.resumeFromIdsList!!.forEach {
                    val timestamp = chooseStartTimestamp(it, request.startTimestamp)
                    add(
                        StreamBasket.create(
                            context, Pair(it.streamName, it.direction), it,
                            timestamp, request, coroutineScope, false
                        )
                    )
                }
            }
        }
    }

    private suspend fun initStreamsInfoFromTime(
        request: SseMessageSearchRequest,
        timestamp: Instant
    ): List<StreamBasket> {
        return mutableListOf<StreamBasket>().apply {
            for (stream in request.stream ?: emptyList()) {
                for (direction in Direction.values()) {
                    val storedMessageId =
                        getFirstMessageIdDifferentDays(timestamp, stream, direction, request.searchDirection)

                    if (storedMessageId != null) {
                        val messageBatch = context.cradleService.getMessageBatchSuspend(storedMessageId)
                        val nearestMessage = getNearestMessage(messageBatch, request.searchDirection, timestamp)

                        add(StreamBasket.create(context,
                            Pair(stream, direction),
                            nearestMessage?.id,
                            timestamp,
                            request,
                            coroutineScope))

                    } else {
                        add(StreamBasket.create(context, Pair(stream, direction), null, timestamp, request, coroutineScope))
                    }
                }
            }
        }
    }

    suspend fun initStreamsInfo(request: SseMessageSearchRequest): List<StreamBasket> {
        return if (request.resumeFromIdsList.isNullOrEmpty()) {
            val startTimestamp = chooseStartTimestamp(
                request.resumeFromId?.let { StoredMessageId.fromString(it) },
                request.startTimestamp
            )
            initStreamsInfoFromTime(request, startTimestamp)
        } else {
            initStreamsInfoFromIds(request)
        }
    }
}