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

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import java.util.*

class MessageBodyBinaryFilter private constructor(
    private var body: List<String>,
    override var negative: Boolean = false
) : Filter<Message> {

    companion object {
        suspend fun build(requestMap: Map<String, List<String>>, cradleService: CradleService): Filter<Message> {
            return MessageBodyBinaryFilter(
                negative = requestMap["${filterInfo.name}-negative"]?.first()?.toBoolean() ?: false,
                body = requestMap["${filterInfo.name}-values"]
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "bodyBinary",
            "matches messages whose binary body contains one of the specified tokens",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "383d 4649, ..."))
            }
        )
    }

    override fun match(element: Message): Boolean {
        return negative.xor(body.any { item ->
            String(Base64.getDecoder().decode(element.bodyBase64)).toLowerCase()?.contains(item.toLowerCase()) ?: false
        })
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}