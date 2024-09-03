/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
 */
package com.exactpro.th2.rptdataprovider.entities.requests

import com.exactpro.cradle.Direction
import com.exactpro.cradle.BookId
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.internal.StreamPointer
import com.exactpro.th2.rptdataprovider.entities.mappers.TimeRelationMapper
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import java.time.Instant


data class SseMessageSearchRequest<RM, PM>(
    val filterPredicate: FilterPredicate<MessageWithMetadata<RM, PM>>,
    val startTimestamp: Instant?,
    val stream: List<String>,
    val searchDirection: TimeRelation,
    val endTimestamp: Instant?,
    val resultCountLimit: Int?,
    val attachedEvents: Boolean,
    val lookupLimitDays: Long?,
    val resumeFromIdsList: List<StreamPointer>,
    val includeProtocols: List<String>?,
    val excludeProtocols: List<String>?,
    val bookId: BookId
) {
    constructor(
        parameters: Map<String, List<String>>,
        filterPredicate: FilterPredicate<MessageWithMetadata<RM, PM>>,
        searchDirection: TimeRelation
    ) : this(
        filterPredicate = filterPredicate,
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"] ?: emptyList(),
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let { TimeRelationMapper.fromHttpString(it) }
            ?: searchDirection,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },

        //FIXME: negative value is used to mark a stream that has not yet started. This needs to be replaced with an explicit flag
        resumeFromIdsList = parameters["messageId"]
            ?.map {
                StoredMessageId.fromString(it).let { id ->
                    StreamPointer(
                        id.sequence, id.sessionAlias, id.direction, id.bookId, id.timestamp, id.sequence > 0
                    )
                }
            }
            ?: emptyList(),

        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        attachedEvents = parameters["attachedEvents"]?.firstOrNull()?.toBoolean() ?: false,
        lookupLimitDays = parameters["lookupLimitDays"]?.firstOrNull()?.toLong(),

        includeProtocols = parameters["includeProtocols"],
        excludeProtocols = parameters["excludeProtocols"],
        bookId = parameters["bookId"]?.firstOrNull()?.let { BookId(it) }
            ?: throw InvalidRequestException("'bookId' is required parameter and it must not be null")
    )

    constructor(
        request: MessageSearchRequest,
        filterPredicate: FilterPredicate<MessageWithMetadata<RM, PM>>,
        bookId: BookId,
        searchDirection: TimeRelation
    ) : this(
        filterPredicate = filterPredicate,
        startTimestamp = if (request.hasStartTimestamp())
            request.startTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,

        stream = if (request.hasStream()) {
            request.stream.listStringList
        } else emptyList<String>(),

        searchDirection = request.searchDirection.let {
            when (it) {
                PREVIOUS -> BEFORE
                else -> searchDirection
            }
        },

        endTimestamp = if (request.hasEndTimestamp())
            request.endTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,

        resultCountLimit = if (request.hasResultCountLimit()) {
            request.resultCountLimit.value
        } else null,


        //FIXME: negative value is used to mark a stream that has not yet started. This needs to be replaced with an explicit flag
        resumeFromIdsList = if (request.messageIdList.isNotEmpty()) {
            request.messageIdList.map {
                StreamPointer(
                    it.sequence,
                    it.connectionId.sessionAlias,
                    grpcDirectionToCradle(it.direction),
                    bookId,
                    it.timestamp.toInstant(),
                    it.sequence > 0
                )
            }
        } else emptyList(),

        attachedEvents = false,

        lookupLimitDays = null,

        includeProtocols = null,

        excludeProtocols = null,
        bookId = bookId
    )

    constructor(parameters: Map<String, List<String>>, filterPredicate: FilterPredicate<MessageWithMetadata<RM, PM>>) : this(
        parameters = parameters,
        filterPredicate = filterPredicate,
        searchDirection = AFTER
    )

    constructor(
        request: MessageSearchRequest,
        filterPredicate: FilterPredicate<MessageWithMetadata<RM, PM>>,
        bookId: BookId
    ) : this(
        request = request,
        filterPredicate = filterPredicate,
        searchDirection = AFTER,
        bookId = bookId
    )

    private fun checkEndTimestamp() {
        if (endTimestamp == null || startTimestamp == null) return

        when(searchDirection) {
            BEFORE -> {
                if (startTimestamp < endTimestamp) {
                    throw InvalidRequestException("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
                }
            }
            AFTER -> {
                if (startTimestamp > endTimestamp) {
                    throw InvalidRequestException("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
                }
            }
        }
    }

    private fun checkStartPoint() {
        if (startTimestamp == null && resumeFromIdsList.isEmpty())
            throw InvalidRequestException("One of the 'startTimestamp' or 'messageId' must not be null")
    }

    private fun checkStreamList() {
        if (stream.isEmpty()) {
            throw InvalidRequestException("Streams list can not be empty")
        }
    }

    private fun checkTimestampAndId() {
        if (startTimestamp != null && resumeFromIdsList.isNotEmpty()) {
            when(searchDirection) {
                BEFORE -> {
                    val pointers = resumeFromIdsList.filter { it.timestamp > startTimestamp }
                    if (pointers.isNotEmpty()) {
                        throw InvalidRequestException(
                            "You cannot specify resume Ids $pointers with timestamp greater than startTimestamp $startTimestamp"
                        )
                    }
                }
                AFTER -> {
                    val pointers = resumeFromIdsList.filter { it.timestamp < startTimestamp }
                    if (pointers.isNotEmpty()) {
                        throw InvalidRequestException(
                            "You cannot specify resume Ids $pointers with timestamp less than startTimestamp $startTimestamp"
                        )
                    }
                }
            }
        }
            throw InvalidRequestException("You cannot specify resume Id and start timestamp at the same time")
    }

    private fun checkResumeIds() {
        if (resumeFromIdsList.size > 1)
            throw InvalidRequestException("you cannot specify more than one id")
    }

    private fun checkIdForStreams() {
        if (resumeFromIdsList.isEmpty()) return

        val mapStreams = stream.associateWith { mutableListOf<StreamPointer>() }
        resumeFromIdsList.forEach { mapStreams[it.stream.name]?.add(it) }
        mapStreams.forEach {
            val messageDirectionList = it.value.map { streamPointer -> streamPointer.stream.direction }

            if (!messageDirectionList.containsAll(Direction.values().toList())) {
                throw InvalidRequestException("ResumeId was not passed for the stream: ${it.key}")
            } else if (messageDirectionList.size > 2) {
                throw InvalidRequestException("Stream ${it.key} has more than two id")
            }
        }
    }

    fun checkIdsRequest() {
        checkStartPoint()
        checkEndTimestamp()
        checkStreamList()
        checkTimestampAndId()
        checkResumeIds()
    }

    fun checkRequest() {
        checkStartPoint()
        checkEndTimestamp()
        checkStreamList()
        checkIdForStreams()
    }
}

