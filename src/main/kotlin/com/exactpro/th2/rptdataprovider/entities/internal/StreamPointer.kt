/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Instant

class CommonStreamName(
    val bookId: BookId,
    val name: String
) {
    @get:JsonIgnore
    internal val fullName = "${bookId.name}:$name"

    override fun toString(): String {
        return fullName
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CommonStreamName

        return fullName == other.fullName
    }

    override fun hashCode(): Int {
        return fullName.hashCode()
    }
}

class StreamName(
    bookId: BookId,
    name: String,
    val direction: Direction,
) {
    val common = CommonStreamName(bookId, name)
    val bookId: BookId
        get() = common.bookId
    val name: String
        get() = common.name

    @get:JsonIgnore
    internal val fullName = "${common.fullName}:$direction"

    override fun toString(): String {
        return fullName
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as StreamName

        return fullName == other.fullName
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + fullName.hashCode()
        return result
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
        stream = StreamName(bookId, streamName, direction),
        sequence = sequence,
        timestamp = timestamp,
        hasStarted = hasStarted
    )
}