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

package com.exactpro.th2.rptdataprovider.entities.filters.events

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class EventTypeFilter private constructor(
    private var type: List<String>,
    override var negative: Boolean = false
) : Filter<BaseEventEntity> {

    companion object {
        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<BaseEventEntity> {
            return EventTypeFilter(
                negative = filterRequest.isNegative(),
                type = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "type",
            "matches events by one of the specified types",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "Send message, ..."))
            }
        )
    }

    override fun match(element: BaseEventEntity): Boolean {
        return negative.xor(type.any { item ->
            element.eventType.toLowerCase().contains(item.toLowerCase())
        })
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}