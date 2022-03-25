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
package com.exactpro.th2.rptdataprovider.entities.requests

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.mappers.TimeRelationMapper
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import java.time.Instant

data class StreamPointer(
    val sequence: Long,
    val streamName: String,
    val direction: Direction,
    val hasStarted: Boolean
)

data class SseMessageSearchRequest(
    val filterPredicate: FilterPredicate<MessageWithMetadata>,
    val startTimestamp: Instant?,
    val stream: List<String>,
    val searchDirection: TimeRelation,
    val endTimestamp: Instant?,
    val resultCountLimit: Int?,
    val attachedEvents: Boolean,
    val lookupLimitDays: Int?,
    val resumeFromIdsList: List<StreamPointer>,
    val includeProtocols: List<String>?,
    val excludeProtocols: List<String>?
) {
    constructor(
        parameters: Map<String, List<String>>,
        filterPredicate: FilterPredicate<MessageWithMetadata>,
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
                        id.index, id.streamName, id.direction, id.index > 0
                    )
                }
            }
            ?: emptyList(),

        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        attachedEvents = parameters["attachedEvents"]?.firstOrNull()?.toBoolean() ?: false,
        lookupLimitDays = parameters["lookupLimitDays"]?.firstOrNull()?.toInt(),

        includeProtocols = parameters["includeProtocols"],
        excludeProtocols = parameters["excludeProtocols"]
    )

    constructor(
        request: MessageSearchRequest,
        filterPredicate: FilterPredicate<MessageWithMetadata>,
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
                PREVIOUS -> TimeRelation.BEFORE
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
                    it.sequence, it.connectionId.sessionAlias, grpcDirectionToCradle(it.direction), it.sequence > 0
                )
            }
        } else emptyList(),

        attachedEvents = false,

        lookupLimitDays = null,

        includeProtocols = null,

        excludeProtocols = null
    )

    constructor(parameters: Map<String, List<String>>, filterPredicate: FilterPredicate<MessageWithMetadata>) : this(
        parameters = parameters,
        filterPredicate = filterPredicate,
        searchDirection = TimeRelation.AFTER
    )

    constructor(request: MessageSearchRequest, filterPredicate: FilterPredicate<MessageWithMetadata>) : this(
        request = request,
        filterPredicate = filterPredicate,
        searchDirection = TimeRelation.AFTER
    )

    private fun checkEndTimestamp() {
        if (endTimestamp == null || startTimestamp == null) return

        if (searchDirection == TimeRelation.AFTER) {
            if (startTimestamp.isAfter(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        } else {
            if (startTimestamp.isBefore(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
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
        if (startTimestamp != null && resumeFromIdsList.isNotEmpty())
            throw InvalidRequestException("You cannot specify resume Id and start timestamp at the same time")
    }

    private fun checkResumeIds() {
        if (resumeFromIdsList.size > 1)
            throw InvalidRequestException("you cannot specify more than one id")
    }

    private fun checkIdForStreams() {
        if (resumeFromIdsList.isEmpty()) return

        val mapStreams = stream.associateWith { mutableListOf<StreamPointer>() }
        resumeFromIdsList.forEach { mapStreams[it.streamName]?.add(it) }
        mapStreams.forEach {
            val set = it.value.map { streamPointer ->  streamPointer.direction }.toSet()
            if (set.size != 2)
                throw InvalidRequestException("ResumeId was not passed for the stream: ${it.key}")
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

