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

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import java.time.Instant

data class MessageSearchRequest(
    val attachedEventId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val stream: List<String>?,
    val messageType: List<String>?,
    val limit: Int,
    val timelineDirection: TimeRelation,
    val messageId: String?,
    val idsOnly: Boolean,
    val probe: Boolean
) {

    companion object {
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>) : this(
        attachedEventId = parameters["attachedEventId"]?.firstOrNull(),
        timestampFrom = parameters["timestampFrom"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        timestampTo = parameters["timestampTo"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"],
        messageType = parameters["messageType"],
        limit = parameters["limit"]?.firstOrNull()?.toInt() ?: 100,

        timelineDirection = parameters["timelineDirection"]?.firstOrNull()?.let {
            asCradleTimeRelation(
                it
            )
        } ?: TimeRelation.AFTER,

        messageId = parameters["messageId"]?.firstOrNull(),
        idsOnly = parameters["idsOnly"]?.firstOrNull()?.toBoolean() ?: true,
        probe = parameters["probe"]?.firstOrNull()?.toBoolean() ?: false
    )
}
