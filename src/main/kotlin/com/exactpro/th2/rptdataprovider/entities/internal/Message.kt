/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import java.time.Instant


data class Message(
    val type: String = "message",
    val timestamp: Instant,
    val direction: Direction,
    val sessionId: String,
    val attachedEventIds: Set<String>,
    val parsedMessageGroup: List<BodyWrapper>?,

    @Suppress("ArrayInDataClass")
    val rawMessageBody: ByteArray,

    var imageType: String?,

    val id: StoredMessageId
) {

    val messageId: String
        get() = id.toString()


    constructor(
        messageWrapper: MessageWrapper,
        parsedMessageGroup: List<BodyWrapper>?,
        events: Set<String>,
        imageType: String? = null
    ) : this(
        id = messageWrapper.id,
        direction = messageWrapper.direction,
        timestamp = messageWrapper.timestamp,
        sessionId = messageWrapper.sessionId,
        attachedEventIds = events,
        rawMessageBody = messageWrapper.rawMessage.body.toByteArray(),
        parsedMessageGroup = parsedMessageGroup,
        imageType = imageType
    )
}

