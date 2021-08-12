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
import com.exactpro.th2.rptdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import mu.KotlinLogging
import java.util.*


class MessageBodyBinaryFilter private constructor(
    private var bodyBinary: List<String>,
    override var negative: Boolean = false,
    override var conjunct: Boolean = false
) : Filter<MessageWithMetadata> {

    companion object {
        private val logger = KotlinLogging.logger { }

        suspend fun build(filterRequest: FilterRequest, cradleService: CradleService): Filter<MessageWithMetadata> {
            return MessageBodyBinaryFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                bodyBinary = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "bodyBinary",
            "matches messages whose binary body contains one of the specified tokens",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "FGW, ..."))
            },
            FilterSpecialType.NEED_BODY_BASE64
        )
    }

    override fun match(element: MessageWithMetadata): Boolean {
        val predicate: (String) -> Boolean = { item ->
            element.message.rawMessageBody?.let {
                String(it.toByteArray()).toLowerCase().contains(item.toLowerCase())
            } ?: false
        }
        return try {
            negative.xor(if (conjunct) bodyBinary.all(predicate) else bodyBinary.any(predicate))
        } catch (e: Exception) {
            logger.warn(e) {}
            false
        }
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}