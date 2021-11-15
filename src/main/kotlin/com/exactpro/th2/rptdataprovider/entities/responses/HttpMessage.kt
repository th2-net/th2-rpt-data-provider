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

import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.Direction
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.protobuf.util.JsonFormat
import java.time.Instant


class HttpBodyWrapper(
    val subsequenceId: List<Int>,
    val protocol: String,
    val messageType: String,
    @JsonRawValue
    val message: String,
    val filtered: Boolean
) {

    companion object {
        suspend fun from(bodyWrapper: BodyWrapper, filtered: Boolean): HttpBodyWrapper {
            return HttpBodyWrapper(
                bodyWrapper.id.subsequenceList,
                bodyWrapper.protocol,
                bodyWrapper.messageType,
                JsonFormat.printer().print(bodyWrapper.message),
                filtered
            )
        }
    }
}


data class HttpMessage(
    val type: String = "message",
    val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val attachedEventIds: Set<String>,
    val messageId: String,
    var body: BodyHttpMessage?,
    var bodyBase64: String?
)

//fixme this is a workaround to revert the old message format
data class BodyHttpMessage(
    @JsonProperty("metadata")
    var metadata: MutableMap<String,Any>?,
    @JsonProperty("fields")
    var fields: MutableMap<String,Any>?,
    @JsonIgnore
    var messageValue: MutableMap<String,Any>? = null
)