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

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class MessageTypeFilter(
    private var type: List<String>,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false,
    override var strict: Boolean = false
) : Filter<MessageWithMetadata> {

    companion object {
        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<MessageWithMetadata> {
            return MessageTypeFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                strict = filterRequest.isStrict(),
                type = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "type",
            "matches messages by one of the specified types",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("strict", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "Heartbeat, ..."))
            }
        )
    }


    private fun predicate(element: BodyWrapper): Boolean {
        val predicate: (String) -> Boolean = { item ->
            if (strict) {
                element.messageType.equals(item, ignoreCase = true)
            } else {
                element.messageType.toLowerCase().contains(item.toLowerCase())
            }
        }
        return negative.xor(if (conjunct) type.all(predicate) else type.any(predicate))
    }

    override fun match(element: MessageWithMetadata): Boolean {
        return element.message.parsedMessageGroup?.let { messageBody ->
            messageBody.forEachIndexed { index, bodyWrapper ->
                predicate(bodyWrapper).also {
                    element.filteredBody[index] = element.filteredBody[index] && it
                }
            }
            element.filteredBody
        }?.any { it } ?: false
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}
