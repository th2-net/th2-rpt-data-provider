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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import java.time.Instant

class BasketCreator(
    private val context: Context,
    private val coroutineScope: CoroutineScope
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }


    data class InitializedBasketsInfo(
        val baskets: List<MessagesBasket>,
        val searchLimit: (Instant) -> Boolean
    )


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


    private fun getTimeSearchLimit(request: SseMessageSearchRequest): ((Instant) -> Boolean) {
        if (request.searchDirection == TimeRelation.AFTER) {
            val futureSearchLimit = Instant.now().nextDay(TimeRelation.AFTER)
            return { timestamp: Instant -> timestamp.isBefore(futureSearchLimit) }
        } else {
            if (request.endTimestamp != null) {
                val pastSearchLimit = request.endTimestamp.nextDay(TimeRelation.BEFORE)
                return { timestamp: Instant -> timestamp.isAfter(pastSearchLimit) }
            } else {
                return { timestamp -> true }
            }
        }
    }


    private suspend fun getFirstMessageIdDifferentDays(
        startTimestamp: Instant,
        streamInfos: List<Pair<String, Direction>>,
        request: SseMessageSearchRequest,
        timeLimit: (Instant) -> Boolean
    ): Map<Pair<String, Direction>, StoredMessageId?> {
        var daysChecking = request.lookupLimitDays
        var isCurrentDay = true
        var timestamp = startTimestamp
        val messageIds: MutableMap<Pair<String, Direction>, StoredMessageId?> = mutableMapOf()
        var firstMessageFound = false

        while (!firstMessageFound && timeLimit(timestamp) && daysChecking?.let { it >= 0 } != false) {
            streamInfos.forEach { (stream, direction) ->
                val messageId = if (isCurrentDay) {
                    getFirstMessageCurrentDay(timestamp, stream, direction)
                } else {
                    context.cradleService.getFirstMessageIdSuspend(
                        timestamp,
                        stream,
                        direction,
                        request.searchDirection
                    )
                }
                messageIds[Pair(stream, direction)] = messageId

                if (messageId != null) {
                    firstMessageFound = true
                }
            }
            daysChecking = daysChecking?.dec()
            isCurrentDay = false
            timestamp = timestamp.nextDay(request.searchDirection)
        }
        return messageIds
    }


    private suspend fun initStreamsInfoFromIds(request: SseMessageSearchRequest): List<MessagesBasket> {
        return coroutineScope {
            mutableListOf<MessagesBasket>().apply {
                request.resumeFromIdsList!!.forEach {
                    val timestamp = chooseStartTimestamp(it, request.startTimestamp)
                    add(
                        MessagesBasket.create(
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
        resumeIdStream: Pair<String, Direction>?,
        timestamp: Instant,
        timeLimit: (Instant) -> Boolean
    ): List<MessagesBasket> {

        val streamInfos = request.stream
            ?.flatMap { stream -> Direction.values().map { Pair(stream, it) } }
            ?: return emptyList()

        val streamToMessageId =
            getFirstMessageIdDifferentDays(timestamp, streamInfos, request, timeLimit)

        return streamToMessageId.map { (streamInfo, storedMessageId) ->
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


    suspend fun initStreamsInfo(request: SseMessageSearchRequest): InitializedBasketsInfo {

        val timeLimit = getTimeSearchLimit(request)

        return if (request.resumeFromIdsList.isNullOrEmpty()) {
            val startTimestamp = chooseStartTimestamp(
                request.resumeFromId?.let { StoredMessageId.fromString(it) },
                request.startTimestamp
            )
            val resumeIdStream = request.resumeFromId
                ?.let { StoredMessageId.fromString(it) }
                ?.let { Pair(it.streamName, it.direction) }

            InitializedBasketsInfo(
                initStreamsInfoFromTime(request, resumeIdStream, startTimestamp, timeLimit),
                timeLimit
            )
        } else {
            InitializedBasketsInfo(initStreamsInfoFromIds(request), timeLimit)
        }
    }
}


private data class DayLimits(
    val requestDay: Instant? = null,
    val dayLimit: Instant? = null,
    val searchDirection: TimeRelation
) {
    val notInit: Boolean
        get() = requestDay == null && dayLimit == null

    companion object {
        private fun getDayLimit(timestamp: Instant, searchDirection: TimeRelation): Instant {
            return if (searchDirection == TimeRelation.AFTER) {
                timestamp.dayEnd()
            } else {
                timestamp.dayStart()
            }
        }

        fun initCurrentDay(timestamp: Instant, searchDirection: TimeRelation): DayLimits {
            return DayLimits(
                requestDay = timestamp,
                dayLimit = getDayLimit(timestamp, searchDirection),
                searchDirection = searchDirection
            )
        }

        fun initNextDay(dayLimits: DayLimits): DayLimits {
            val nextDay = dayLimits.requestDay!!.nextDay(dayLimits.searchDirection)
            return DayLimits(
                requestDay = nextDay,
                dayLimit = getDayLimit(nextDay, dayLimits.searchDirection),
                searchDirection = dayLimits.searchDirection
            )
        }
    }
}


data class BasketInitializer(
    private val context: Context,
    private val coroutineScope: CoroutineScope,
    private val request: SseMessageSearchRequest,
    private val timeLimit: (Instant) -> Boolean
) {

    private var dayLimits = DayLimits(searchDirection = request.searchDirection)
    var allBasketsInit = false
        private set

    private fun getFirstMessageInDay(
        messageBatch: Collection<StoredMessage>,
        timelineDirection: TimeRelation
    ): StoredMessage? {
        if (messageBatch.isEmpty()) return null
        return if (timelineDirection == TimeRelation.AFTER)
            messageBatch.first()
        else
            messageBatch.last()
    }


    private suspend fun tryToInitBaskets(
        messageBaskets: List<MessagesBasket>,
        timestamp: Instant
    ) {
        messageBaskets.map { basket ->
            val storedMessageId = context.cradleService.getFirstMessageIdSuspend(
                timestamp,
                basket.stream.first,
                basket.stream.second,
                request.searchDirection
            )

            if (storedMessageId != null) {
                val messageBatch = context.cradleService.getMessageBatchSuspend(storedMessageId)
                val nearestMessage = getFirstMessageInDay(messageBatch, request.searchDirection)

                nearestMessage?.let { basket.tryToInitBasket(it.id) }
            }
        }
    }

    private fun changeCurrentDay(timestamp: Instant?) {
        dayLimits = if (dayLimits.notInit && timestamp != null) {
            DayLimits.initCurrentDay(timestamp, request.searchDirection)
        } else {
            DayLimits.initNextDay(dayLimits)
        }
    }

    private fun changeCurrentDay(wrapper: MessageWrapper?) {
        changeCurrentDay(wrapper?.message?.timestamp)
    }

    private suspend fun tryToInitBaskets(messageBaskets: List<MessagesBasket>) {
        if (!allBasketsInit) {
            tryToInitBaskets(messageBaskets, dayLimits.requestDay!!)
        }
        allBasketsInit = messageBaskets.all { it.canInit() }
    }

    private fun isDayChanged(timestamp: Instant): Boolean {
        return if (request.searchDirection == TimeRelation.AFTER)
            timestamp.isAfterOrEqual(dayLimits.dayLimit!!)
        else
            timestamp.isBeforeOrEqual(dayLimits.dayLimit!!)
    }

    fun isDayChanged(wrapper: MessageWrapper?): Boolean {
        if (wrapper == null) return false
        return isDayChanged(wrapper.message.timestamp)
    }

    suspend fun dayChangedInitBaskets(wrapper: MessageWrapper?, messageBaskets: List<MessagesBasket>) {
        if (isDayChanged(wrapper) && !allBasketsInit && wrapper?.message?.let { timeLimit(it.timestamp) } != false) {
            changeCurrentDay(wrapper)
            tryToInitBaskets(messageBaskets)
        }
    }

    suspend fun tryToInitBasketsInFuture(messageBaskets: List<MessagesBasket>) {
        val nowDay = Instant.now()
        if (isDayChanged(nowDay)) {
            changeCurrentDay(nowDay)
            tryToInitBaskets(messageBaskets)
        }
    }
}
