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

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import java.time.Instant

data class SseEventSearchRequest(
    val filterPredicate: FilterPredicate<BaseEventEntity>,
    val startTimestamp: Instant?,
    val parentEvent: ProviderEventId?,
    val searchDirection: TimeRelation,
    val endTimestamp: Instant?,
    val resumeFromId: String?,
    val resultCountLimit: Int?,
    val keepOpen: Boolean,
    val limitForParent: Long?,
    val metadataOnly: Boolean,
    val attachedMessages: Boolean
) {
    companion object {
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>, filterPredicate: FilterPredicate<BaseEventEntity>) : this(
        filterPredicate = filterPredicate,
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        parentEvent = parameters["parentEvent"]?.firstOrNull()?.let { ProviderEventId(it) },
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let {
            asCradleTimeRelation(it)
        } ?: TimeRelation.AFTER,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resumeFromId = parameters["resumeFromId"]?.firstOrNull(),
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        keepOpen = parameters["keepOpen"]?.firstOrNull()?.toBoolean() ?: false,
        limitForParent = parameters["limitForParent"]?.firstOrNull()?.toLong(),
        metadataOnly = parameters["metadataOnly"]?.firstOrNull()?.toBoolean() ?: true,
        attachedMessages = parameters["attachedMessages"]?.firstOrNull()?.toBoolean() ?: false
    )

    constructor(request: EventSearchRequest, filterPredicate: FilterPredicate<BaseEventEntity>) : this(
        filterPredicate = filterPredicate,
        startTimestamp = if (request.hasStartTimestamp())
            request.startTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,
        parentEvent = if (request.hasParentEvent()) {
            ProviderEventId(request.parentEvent.id)
        } else null,
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
            request.resumeFromId.id
        } else null,
        resultCountLimit = if (request.hasResultCountLimit()) {
            request.resultCountLimit.value
        } else null,
        keepOpen = if (request.hasKeepOpen()) {
            request.keepOpen.value
        } else false,
        limitForParent = if (request.hasLimitForParent()) {
            request.limitForParent.value
        } else null,
        metadataOnly = if (request.hasMetadataOnly()) {
            request.metadataOnly.value
        } else true,
        attachedMessages = if (request.hasAttachedMessages()) {
            request.attachedMessages.value
        } else false
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
        if (startTimestamp == null && resumeFromId == null)
            throw InvalidRequestException("One of the 'startTimestamp' or 'resumeFromId' must not be null")
    }

    fun checkRequest() {
        checkStartPoint()
        checkEndTimestamp()
    }
}
