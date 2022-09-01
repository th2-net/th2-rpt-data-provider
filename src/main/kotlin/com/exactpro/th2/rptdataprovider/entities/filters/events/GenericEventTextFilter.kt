/*******************************************************************************
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService


class GenericEventTextFilter(
    private var values: List<String>,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false,
    override var strict: Boolean = false
) : Filter<BaseEventEntity> {

    companion object {
        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<BaseEventEntity> {
            return GenericEventTextFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                strict = filterRequest.isStrict(),
                values = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo =
            FilterInfo("event_generic", "matches events by name, body or type", mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("strict", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "NewOrderSingle, ..."))
            })
    }

    private val bodyFilter = EventBodyFilter(body = values, conjunct = conjunct, strict = strict)
    private val nameFilter = EventNameFilter(name = values, conjunct = conjunct, strict = strict)
    private val typeFilter = EventTypeFilter(type = values, conjunct = conjunct, strict = strict)

    override fun match(element: BaseEventEntity): Boolean {
        return negative.xor(
            typeFilter.match(element) || nameFilter.match(element) || bodyFilter.match(element)
        )
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }

}

