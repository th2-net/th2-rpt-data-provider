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

data class SseMessageSearchRequest(
    val attachedEventIds: List<String>?,
    val startTimestamp: Instant,
    val stream: List<String>?,
    val type: List<String>?,
    val negativeTypeFilter: Boolean,
    val searchDirection: TimeRelation,
    val resultCountLimit: Int,
    val timeLimit: Long
) {

    companion object {
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>) : this(
        attachedEventIds = parameters["attachedEventIds"],
        startTimestamp = parameters["startTimestamp"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) }!! ,
        stream = parameters["stream"],
        type = parameters["type"],
        negativeTypeFilter = parameters["negativeTypeFilter"]?.first()?.toBoolean() ?: false,
        searchDirection = parameters["searchDirection"]?.let {
            asCradleTimeRelation(
                it.first()
            )
        } ?: TimeRelation.AFTER,
        resultCountLimit = parameters["resultCountLimit"]?.first()?.toInt() ?: 100,
        timeLimit = parameters["timeLimit"]?.first()?.toLong() ?: 5000L
    )
}
