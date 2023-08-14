/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import java.time.Instant


data class StreamName(
    val name: String,
    val direction: Direction,
    val bookId: BookId
) {
    private val fullName = "$name:$direction"

    override fun toString(): String {
        return fullName
    }
}


data class StreamPointer(
    val stream: StreamName,
    val sequence: Long,
    val timestamp: Instant,
    val hasStarted: Boolean
) {
    constructor(
        sequence: Long,
        streamName: String,
        direction: Direction,
        bookId: BookId,
        timestamp: Instant,
        hasStarted: Boolean
    ) : this(
        stream = StreamName(streamName, direction, bookId),
        sequence = sequence,
        timestamp = timestamp,
        hasStarted = hasStarted
    )
}

