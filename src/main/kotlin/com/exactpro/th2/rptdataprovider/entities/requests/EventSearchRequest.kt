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
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import kotlin.math.max

data class EventSearchRequest(
    val attachedMessageId: String?,
    val timestampFrom: Instant,
    val timestampTo: Instant,
    val name: List<String>?,
    val type: List<String>?,
    val flat: Boolean,
    val parentEvent: String?,
    val probe: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedMessageId = parameters["attachedMessageId"]?.firstOrNull(),
        timestampFrom = parameters["timestampFrom"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) }
            ?: throw InvalidRequestException("Required parameter 'timestampFrom' not specified"),
        timestampTo = parameters["timestampTo"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) }
            ?: throw InvalidRequestException("Required parameter 'timestampTo' not specified"),
        name = parameters["name"],
        type = parameters["type"],
        flat = parameters["flat"]?.firstOrNull()?.toBoolean() ?: false,
        parentEvent = parameters["parentEvent"]?.firstOrNull(),
        probe = parameters["probe"]?.firstOrNull()?.toBoolean() ?: false
    )

    fun checkTimestamps() {
        val timeDistance = timestampTo.minusMillis(timestampFrom.toEpochMilli()).toEpochMilli()
        val maxTimeDistance = TimeUnit.HOURS.toMillis(24)
        if (timeDistance >= maxTimeDistance)
            throw InvalidRequestException("Distance between timestampFrom: $timestampFrom and timestampTo: $timestampTo must be less then 24 hours")
    }
}
