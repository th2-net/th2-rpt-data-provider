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

import java.time.Instant

data class EventSearchRequest(
    val attachedMessageId: String?,
    val timestampFrom: Instant,
    val timestampTo: Instant,
    val name: List<String>?,
    val type: List<String>?,
    val flat: Boolean,
    val parentEvent: String?
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedMessageId = parameters["attachedMessageId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) }!!,
        timestampTo = parameters["timestampTo"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) }!!,
        name = parameters["name"],
        type = parameters["type"],
        flat = parameters["flat"]?.first()?.toBoolean() ?: false,
        parentEvent = parameters["parentEvent"]?.first()
    )
}
