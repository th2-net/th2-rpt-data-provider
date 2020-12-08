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

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.entities.filters.SimpleFilter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode

class AttachedMessageFilter(
    private val eventIds: Collection<StoredTestEventId>,
    override val negative: Boolean
) : SimpleFilter<EventTreeNode> {

    companion object {
        val filterInfo = FilterInfo(
            "attachedMessageId",
            "matches events by one of the attached message id",
            mutableListOf<Parameter>().apply {
                add(Parameter("invert", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "arfq01fix01:second:1604492791034943949, ..."))
            }
        )
    }

    override fun match(element: EventTreeNode): Boolean {
        return negative.xor(eventIds.contains(StoredTestEventId(element.eventId)))
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}


