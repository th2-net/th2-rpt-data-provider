/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.filters.messages

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import kotlinx.coroutines.runBlocking

class AttachedEventFilters(
    requestMap: Map<String, List<String>>,
    cradleService: CradleService
) : Filter<Message>(requestMap, cradleService) {

    private lateinit var messagesFromAttachedId: Collection<Collection<StoredMessageId>>
    override var negative: Boolean = false

    init {
        negative = requestMap["${filterInfo.name}-negative"]?.first()?.toBoolean() ?: false
        runBlocking {
            messagesFromAttachedId = requestMap["${filterInfo.name}-values"]
                ?.map { cradleService.getMessageIdsSuspend(StoredTestEventId(it)) }
                ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
        }
    }

    companion object {
        val filterInfo = FilterInfo(
            "attachedEventIds",
            "matches messages by one of the attached event id",
            mutableListOf<Parameter>().apply {
                add(Parameter("invert", FilterParameterType.BOOLEAN, false, null))
                add(
                    Parameter(
                        "values",
                        FilterParameterType.STRING_LIST,
                        null,
                        "aae36f85-e638-482d-b996-b4bf710048b8, ..."
                    )
                )
            }
        )
    }

    override fun match(element: Message): Boolean {
        return negative.xor(messagesFromAttachedId.let { messagesFromEventId ->
            val messageId = StoredMessageId.fromString(element.messageId)
            messagesFromEventId.any { it.contains(messageId) }
        })
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}
