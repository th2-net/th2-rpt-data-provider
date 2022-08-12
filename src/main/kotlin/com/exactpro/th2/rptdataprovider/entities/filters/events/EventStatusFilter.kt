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

package com.exactpro.th2.rptdataprovider.entities.filters.events

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class EventStatusFilter private constructor(
    private var status: Boolean,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false,
    override var strict: Boolean = false
) : Filter<BaseEventEntity> {
    companion object {
        private const val failedStatus = "failed"
        private const val passedStatus = "passed"

        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<BaseEventEntity> {
            val status = filterRequest.getValues()?.first()?.toLowerCase()
                ?: throw InvalidRequestException("'${filterInfo.name}-value' cannot be empty")

            if (failedStatus != status && passedStatus != status) {
                throw InvalidRequestException("'${filterInfo.name}-value' wrong value")
            }

            return EventStatusFilter(
                negative = filterRequest.isNegative(),
                status = status == passedStatus
            )
        }

        val filterInfo = FilterInfo(
            "status",
            "matches events by one of the status",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("value", FilterParameterType.STRING, null, "Failed, Passed"))
            }
        )
    }

    override fun match(element: BaseEventEntity): Boolean {
        return negative.xor(status == element.successful)
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }

}