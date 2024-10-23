/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.fasterxml.jackson.annotation.JsonIgnore
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant

data class StreamInfo(val streamPointer: CommonStreamName, @JsonIgnore val messageId: StoredMessageId? = null) {
    @JsonIgnore
    private val lastMessage = messageId ?: let {
        // set sequence to a negative value if stream has not started to avoid mistaking it for the default value
        // FIXME change interface to make that case explicit
        LOGGER.trace { "lastElement is null - StreamInfo should be build as if the stream $streamPointer has no messages" }
        StoredMessageId(
            streamPointer.bookId,
            streamPointer.name,
            Direction.FIRST,
            Instant.ofEpochMilli(0),
            0
        )
    }

    val lastElement = lastMessage.toString()

    fun convertToProto(): Stream {
        return lastMessage.run {
            LOGGER.trace { "stream $streamPointer - lastElement is $sequence" }
            Stream.newBuilder()
                .setDirection(cradleDirectionToGrpc(direction))
                .setSession(sessionAlias)
                .setLastId(this.convertToProto())
                .build()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}