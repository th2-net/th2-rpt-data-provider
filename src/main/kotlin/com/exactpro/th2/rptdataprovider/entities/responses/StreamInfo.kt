/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonUnwrapped
import java.time.Instant

data class StreamInfo(
    @JsonUnwrapped
    val stream: StreamName,
    @JsonIgnore
    val lastElement: StoredMessageId? = null
) {

    @JsonProperty("lastId")
    val lastId = lastElement?.toString()

    fun convertToProto(): Stream {
        return Stream.newBuilder()
            .setDirection(cradleDirectionToGrpc(stream.direction))
            .setSession(stream.session)
            .also { builder ->
                lastElement?.let { builder.setLastId(it.convertToProto()) }
            }.build()
    }
}