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

data class MessageIdWithSubsequences(
    val messageId: StoredMessageId,
    val subsequences: List<Int>
) {
    companion object {

        private const val MESSAGE_ID_SUB_ID_SEPARATOR = "."
        private const val IDS_DELIMITER = ':'

        fun from(id: String): MessageIdWithSubsequences {
            val messageIndexStart = id.indexOfLast { it == IDS_DELIMITER }
            val messageIndexSubstring = id.substring(messageIndexStart + 1, id.length)
            val messageSequenceAndDirection = id.substring(0, messageIndexStart)

            val idList = messageIndexSubstring.split(MESSAGE_ID_SUB_ID_SEPARATOR)

            val messageId = buildString {
                append(messageSequenceAndDirection)
                append(IDS_DELIMITER)
                append(idList.first())
            }

            val storedId = StoredMessageId.fromString(messageId)

            return MessageIdWithSubsequences(
                messageId = storedId,
                subsequences = idList.subList(1, idList.size).map { it.toInt() }
            )
        }
    }
}
