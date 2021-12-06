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

package com.exactpro.th2.rptdataprovider.entities.filters.messages

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class AttachedEventFilters private constructor(
    private var messagesFromAttachedId: Set<String>,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false
) : Filter<MessageWithMetadata> {

    companion object {

        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<MessageWithMetadata> {
            return AttachedEventFilters(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                messagesFromAttachedId = filterRequest.getValues()
                    ?.map {
                        cradleService.getMessageIdsSuspend(
                            StoredTestEventId.fromString(it)
                        )
                            .map { id -> id.toString() }
                            .toSet()
                    }
                    ?.reduce { set, element ->
                        if (filterRequest.isConjunct()) {
                            set intersect element
                        } else {
                            set union element
                        }
                    }
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "attachedEventIds",
            "matches messages by one of the attached event id",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(
                    Parameter(
                        "values",
                        FilterParameterType.STRING_LIST,
                        null,
                        "aae36f85-e638-482d-b996-b4bf710048b8, ..."
                    )
                )
            },
            FilterSpecialType.NEED_ATTACHED_EVENTS
        )
    }

    override fun match(element: MessageWithMetadata): Boolean {
        return negative.xor(messagesFromAttachedId.contains(element.message.messageId))
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}
