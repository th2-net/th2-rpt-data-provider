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

package com.exactpro.th2.rptdataprovider

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageID

fun cradleDirectionToGrpc(direction: Direction): com.exactpro.th2.common.grpc.Direction {
    return if (direction == Direction.FIRST)
        com.exactpro.th2.common.grpc.Direction.FIRST
    else
        com.exactpro.th2.common.grpc.Direction.SECOND
}

fun grpcDirectionToCradle(direction: com.exactpro.th2.common.grpc.Direction): Direction {
    return if (direction == com.exactpro.th2.common.grpc.Direction.FIRST)
        Direction.FIRST
    else
        Direction.SECOND
}

fun providerDirectionToCradle(direction: com.exactpro.th2.rptdataprovider.entities.internal.Direction?): Direction? {
    return direction?.let {
        if (it == com.exactpro.th2.rptdataprovider.entities.internal.Direction.IN) {
            Direction.FIRST
        } else {
            Direction.SECOND
        }
    }
}

fun grpcMessageIdToString(messageId: MessageID): String {
    val storedMessageId = StoredMessageId(
        messageId.connectionId.sessionAlias,
        grpcDirectionToCradle(messageId.direction),
        messageId.sequence
    )

    return messageId.subsequenceList.joinToString(
        prefix = "$storedMessageId.", separator = "."
    )
}
