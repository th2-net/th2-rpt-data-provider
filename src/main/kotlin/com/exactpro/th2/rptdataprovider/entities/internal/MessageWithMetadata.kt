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

import com.exactpro.th2.common.grpc.MessageID

interface DataWrapper<T> {
    val message: T
    var finalFiltered: Boolean
}

data class MessageWithMetadata(
    override val message: Message,
    val filteredBody: MutableList<Boolean> = mutableListOf(),
    override var finalFiltered: Boolean = true
) : DataWrapper<Message> {

    constructor(message: Message) : this(
        message = message,
        filteredBody = message.parsedMessageGroup?.let { List(it.size) { true } as MutableList } ?: mutableListOf()
    )

    constructor(message: Message, id: MessageID) : this(message) {
        message.parsedMessageGroup?.let { body ->
            val messageIndexWithSubsequence = body.indexOfFirst { it.id == id }
            if (messageIndexWithSubsequence >= 0) {
                for (i in 0 until filteredBody.size) {
                    filteredBody[i] = false
                }
                filteredBody[messageIndexWithSubsequence] = true
            }
        }
    }

    fun getMessagesWithMatches(): List<Pair<BodyWrapper, Boolean>>? {
        return message.parsedMessageGroup?.let {
            it zip filteredBody
        }
    }
}
