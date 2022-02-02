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
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.handlers.StreamName

data class StreamInfo(val stream: StreamName, val lastElement: StoredMessageId? = null) {
    fun convertToProto(): Stream {
        return Stream.newBuilder()
            .setDirection(cradleDirectionToGrpc(stream.direction))
            .setSession(stream.name)
            .also { builder ->
                lastElement?.let {
                    builder.setLastId(it.convertToProto())

                    // set sequence to a negative value if stream has not started to avoid mistaking it for the default value
                    // FIXME change interface to make that case explicit
                        ?: MessageID
                            .newBuilder()
                            .setSequence(-1)
                            .setDirection(cradleDirectionToGrpc(stream.direction))
                            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(stream.name).build())
                            .build()
                }
            }.build()
    }
}
