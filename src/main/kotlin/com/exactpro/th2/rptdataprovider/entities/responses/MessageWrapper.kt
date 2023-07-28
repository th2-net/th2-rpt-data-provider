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
package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.internal.Direction
import java.time.Instant

data class MessageWrapper<RM>(
    val id: StoredMessageId,
    val rawMessage: RM,
    val book: String,
    val sessionGroup: String,
    val sessionAlias: String,
    val direction: Direction,
    val timestamp: Instant,

    @Suppress("ArrayInDataClass")
    val storedContent: ByteArray
) {
    constructor(
        rawStoredMessage: StoredMessage,
        sessionGroup: String,
        rawMessage: RM
    ) : this(
        id = rawStoredMessage.id,
        rawMessage = rawMessage,
        book = rawStoredMessage.bookId.name,
        sessionGroup = sessionGroup,
        sessionAlias = rawStoredMessage.sessionAlias,
        direction = Direction.fromStored(rawStoredMessage.direction ?: com.exactpro.cradle.Direction.FIRST),
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        storedContent = rawStoredMessage.content,
    )
}