/*******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.reportdataprovider.entities

import java.time.Instant

enum class TimelineDirection(val alias: String) {
    PREVIOUS("previous"), NEXT("next");

    companion object {
        fun byAlias(alias: String): TimelineDirection {
            try {
                return values().first { it.alias == alias }
            } catch (e: NoSuchElementException) {
                throw IllegalArgumentException("'$alias' is not a valid timeline direction")
            }
        }
    }
}

data class MessageSearchRequest(
    val attachedEventId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val stream: List<String>?,
    val messageType: List<String>?,
    val limit: Int,
    val timelineDirection: TimelineDirection,
    val messageId: String?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedEventId = parameters["attachedEventId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        timestampTo = parameters["timestampTo"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"],
        messageType = parameters["messageType"],
        limit = parameters["limit"]?.first()?.toInt() ?: 100,

        timelineDirection = parameters["timelineDirection"]
            ?.let { TimelineDirection.byAlias(it.first()) } ?: TimelineDirection.NEXT,

        messageId = parameters["messageId"]?.first(),
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: true
    )
}

data class EventSearchRequest(
    val attachedMessageId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val name: List<String>?,
    val type: List<String>?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedMessageId = parameters["attachedMessageId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        timestampTo = parameters["timestampTo"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        name = parameters["name"],
        type = parameters["type"],
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: false
    )
}
