﻿/*******************************************************************************
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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset

class BasketCreator(
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

    private fun getNearestMessage(
        messageBatch: Collection<StoredMessage>,
        timelineDirection: TimeRelation,
        timestamp: Instant
    ): StoredMessage? {
        if (messageBatch.isEmpty()) return null
        return if (timelineDirection == TimeRelation.AFTER)
            messageBatch.find { it.timestamp.isAfterOrEqual(timestamp) } ?: messageBatch.lastOrNull()
        else
            messageBatch.findLast { it.timestamp.isBeforeOrEqual(timestamp) } ?: messageBatch.firstOrNull()
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

    private suspend fun getFirstMessageInStream(stream: String, direction: Direction): StoredMessage? {
        val index = context.cradleService.getFirstMessageIndex(stream, direction)
        if (index == -1L) return null
        val messageId = StoredMessageId(stream, direction, index)
        return context.cradleService.getMessageSuspend(messageId)
    }

    private fun getTimeSearchLimit(request: SseMessageSearchRequest, lastTimestamp: Instant?): ((Instant) -> Boolean) {
        if (request.searchDirection == TimeRelation.AFTER) {
            val futureSearchLimit = nextDay(Instant.now(), TimeRelation.AFTER)
            return { timestamp: Instant -> timestamp.isBefore(futureSearchLimit) }
        } else {
            if (request.endTimestamp != null) {
                val pastSearchLimit = nextDay(request.endTimestamp, TimeRelation.BEFORE)

                return { timestamp: Instant ->
                    timestamp.isAfter(pastSearchLimit)
                            || lastTimestamp?.let { timestamp.isAfterOrEqual(it) } ?: true
                }
            } else {
                return { timestamp: Instant -> lastTimestamp?.let { timestamp.isAfterOrEqual(it) } ?: true }
            }
        }
    }

    private suspend fun getFirstMessageIdDifferentDays(
        startTimestamp: Instant,
        stream: String,
        direction: Direction,
        request: SseMessageSearchRequest
    ): StoredMessageId? {
        var daysChecking = request.lookupLimitDays
        var isCurrentDay = true
        var timestamp = startTimestamp
        var messageId: StoredMessageId? = null
        val firstMessageInStream = getFirstMessageInStream(stream, direction)
        val timeLimit = getTimeSearchLimit(request, firstMessageInStream?.timestamp)

        while (messageId == null && timeLimit(timestamp) && daysChecking?.let { it >= 0 } != false) {
            messageId =
                if (isCurrentDay) {
                    getFirstMessageCurrentDay(timestamp, stream, direction)
                } else {
                    context.cradleService.getFirstMessageIdSuspend(
                        timestamp,
                        stream,
                        direction,
                        request.searchDirection
                    )
                }
            daysChecking = daysChecking?.dec()
            isCurrentDay = false
            timestamp = nextDay(timestamp, request.searchDirection)
        }
        return messageId
    }

    private suspend fun initStreamsInfoFromIds(
        request: SseMessageSearchRequest,
        streamInfos: List<Pair<String, Direction>>
    ): List<MessagesBasket> {
        return coroutineScope {
            val resumeFromIdMap = request.resumeFromIdsList
                ?.associateBy { Pair(it.streamName, it.direction) } ?: emptyMap()

            streamInfos.map { stream ->
                val messageId = resumeFromIdMap[stream]
                if (messageId == null) {
                    MessagesBasket.create(
                        context, stream, messageId,
                        request.startTimestamp ?: Instant.now(),
                        request, coroutineScope, true
                    )
                } else {
                    val timestamp = chooseStartTimestamp(messageId, request.startTimestamp)
                    MessagesBasket.create(
                        context, stream, messageId,
                        timestamp, request, coroutineScope, true
                    )
                }
            }
        }
    }

    private suspend fun initStreamsInfoFromTime(
        request: SseMessageSearchRequest,
        streams: List<Pair<String, Direction>>,
        resumeIdStream: Pair<String, Direction>?,
        timestamp: Instant
    ): List<MessagesBasket> {
        return streams.map { (stream, direction) ->
            val storedMessageId =
                getFirstMessageIdDifferentDays(timestamp, stream, direction, request)

            val streamInfo = Pair(stream, direction)
            val isFirstPull = streamInfo != resumeIdStream

            if (storedMessageId != null) {
                val messageBatch = context.cradleService.getMessageBatchSuspend(storedMessageId)
                val nearestMessage = getNearestMessage(messageBatch, request.searchDirection, timestamp)

                MessagesBasket.create(
                    context, streamInfo, nearestMessage?.id,
                    timestamp, request, coroutineScope, isFirstPull
                )
            } else {
                MessagesBasket.create(
                    context, streamInfo, null, timestamp, request,
                    coroutineScope, isFirstPull
                )
            }
        }
    }


    suspend fun initStreamsInfo(request: SseMessageSearchRequest): List<MessagesBasket> {
        val streamInfos = request.stream
            ?.flatMap { stream -> Direction.values().map { Pair(stream, it) } }
            ?: return emptyList()

        return if (request.resumeFromIdsList.isNullOrEmpty()) {
            val startTimestamp = chooseStartTimestamp(
                request.resumeFromId?.let { StoredMessageId.fromString(it) },
                request.startTimestamp
            )
            val resumeIdStream = request.resumeFromId
                ?.let { StoredMessageId.fromString(it) }
                ?.let { Pair(it.streamName, it.direction) }

            initStreamsInfoFromTime(request, streamInfos, resumeIdStream, startTimestamp)
        } else {
            initStreamsInfoFromIds(request, streamInfos)
        }
    }
}