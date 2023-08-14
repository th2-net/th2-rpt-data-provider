/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toProto
import com.google.protobuf.util.JsonFormat


abstract class BodyWrapper<PM>(
    val id: MessageID,
    val protocol: String,
    val messageType: String,
    val message: PM
) {
    abstract fun toJson(): String
}

class ProtoBodyWrapper(
    message: Message,
): BodyWrapper<Message>(
    message.metadata.id,
    message.metadata.protocol,
    message.metadata.messageType,
    message
) {
    override fun toJson(): String = JsonFormat.printer().print(message)
}

class TransportBodyWrapper(
    message: ParsedMessage,
    private val book: String,
    private val sessionGroup: String,
): BodyWrapper<ParsedMessage>(
    message.id.toProto(book, sessionGroup),
    message.protocol,
    message.type,
    message
) {
    // FIXME: Optimize Serialization
    override fun toJson(): String = JsonFormat.printer().print(message.toProto(book, sessionGroup))
}