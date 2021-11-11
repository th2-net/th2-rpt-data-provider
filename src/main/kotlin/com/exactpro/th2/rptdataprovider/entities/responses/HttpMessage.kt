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

package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.Direction
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.grpcMessageIdToString
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.util.JsonFormat
import java.time.Instant


class HttpBodyWrapper(
    val match: Boolean,
    val id: String,
    @JsonRawValue
    val message: String
) {

    companion object {
        suspend fun from(bodyWrapper: BodyWrapper, match: Boolean): HttpBodyWrapper {
            return HttpBodyWrapper(
                match,
                grpcMessageIdToString(bodyWrapper.id),
                JsonFormat.printer().print(bodyWrapper.message)
            )
        }
    }
}


data class HttpMessage(
    val type: String = "message",
    val id: String,
//    @com.fasterxml.jackson.annotation.JsonFormat(
//        shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING,
//        pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"
//    )
    val timestamp: Instant,
    val sessionId: String,
    val direction: Direction?,
    val sequence: String,
    val attachedEventIds: Set<String>,
    var rawMessageBase64: String?,
    var parsedMessages: List<HttpBodyWrapper>?
)
