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

package com.exactpro.th2.rptdataprovider.entities.filters.messages

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.internal.BodyWrapper
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import java.util.*

class MessageBodyFilter<RM, PM> private constructor(
    private var body: List<String>,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false
) : Filter<MessageWithMetadata<RM, PM>> {

    companion object {
        @Suppress("RedundantSuspendModifier")
        suspend fun <RM, PM> build(filterRequest: FilterRequest, @Suppress("UNUSED_PARAMETER") cradleService: CradleService): Filter<MessageWithMetadata<RM, PM>> {
            return MessageBodyFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                body = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "body",
            "matches messages whose body contains one of the specified tokens",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "FGW, ..."))
            },
            FilterSpecialType.NEED_JSON_BODY
        )
    }

    private fun predicate(element: BodyWrapper<PM>): Boolean {
        val predicate: (String) -> Boolean = { item ->
            element.toJson().lowercase(Locale.getDefault())
                .contains(item.lowercase(Locale.getDefault()))
        }
        return negative.xor(if (conjunct) body.all(predicate) else body.any(predicate))
    }

    override fun match(element: MessageWithMetadata<RM, PM>): Boolean {
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
