/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.filters.events

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import java.util.*

class EventNameFilter private constructor(
    private var name: List<String>, override var negative: Boolean = false, override var conjunct: Boolean = false
) : Filter<BaseEventEntity> {

    companion object {
        @Suppress("RedundantSuspendModifier")
        suspend fun build(filterRequest: FilterRequest, @Suppress("UNUSED_PARAMETER") cradleService: CradleService): Filter<BaseEventEntity> {
            return EventNameFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                name = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "name",
            "matches events by one of the specified names",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "NewOrderSingle, ..."))
            }
        )
    }

    override fun match(element: BaseEventEntity): Boolean {
        val predicate: (String) -> Boolean = { item ->
            element.eventName.lowercase(Locale.getDefault()).contains(item.lowercase(Locale.getDefault()))
        }
        return negative.xor(if (conjunct) name.all(predicate) else name.any(predicate))
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }

}

