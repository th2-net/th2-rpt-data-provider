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
package com.exactpro.th2.rptdataprovider.entities.requests

import com.exactpro.cradle.Direction.*
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.dataprovider.grpc.TimeRelation.*
import com.exactpro.th2.rptdataprovider.grpcDirectionToCradle
import java.time.Instant

data class SseMessageSearchRequest(
    val filterPredicate: FilterPredicate<Message>,
    val startTimestamp: Instant?,
    val stream: List<String>?,
    val searchDirection: TimeRelation,
    val endTimestamp: Instant?,
    val resumeFromId: String?,
    val resultCountLimit: Int?,
    val keepOpen: Boolean,
    val resumeFromIdsList: List<StoredMessageId>?
) {

    companion object {
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>, filterPredicate: FilterPredicate<Message>) : this(
        filterPredicate = filterPredicate,
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"],
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let {
            asCradleTimeRelation(
                it
            )
        } ?: TimeRelation.AFTER,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resumeFromId = parameters["resumeFromId"]?.firstOrNull(),
        resumeFromIdsList = parameters["messageId"]?.map { StoredMessageId.fromString(it) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        keepOpen = parameters["keepOpen"]?.firstOrNull()?.toBoolean() ?: false
    )

    constructor(request: MessageSearchRequest, filterPredicate: FilterPredicate<Message>) : this(
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
                else -> TimeRelation.AFTER
            }
        },

        endTimestamp = if (request.hasEndTimestamp())
            request.endTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,

        resumeFromId = if (request.hasResumeFromId()) {
            request.resumeFromId.let {
                StoredMessageId(
                    it.connectionId.sessionAlias,
                    grpcDirectionToCradle(it.direction),
                    it.sequence
                ).toString()
            }
        } else null,

        resultCountLimit = if (request.hasResultCountLimit()) {
            request.resultCountLimit.value
        } else null,

        keepOpen = if (request.hasKeepOpen()) {
            request.keepOpen.value
        } else false,

        resumeFromIdsList = if (request.messageIdList.isNotEmpty()) {
            request.messageIdList.map {
                StoredMessageId(
                    it.connectionId.sessionAlias,
                    grpcDirectionToCradle(it.direction),
                    it.sequence
                )
            }
        } else null
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
        if (startTimestamp == null && resumeFromId == null && resumeFromIdsList == null)
            throw InvalidRequestException("One of the 'startTimestamp' or 'resumeFromId' or 'messageId' must not be null")
    }

    fun checkRequest() {
        checkStartPoint()
        checkEndTimestamp()
    }
}

