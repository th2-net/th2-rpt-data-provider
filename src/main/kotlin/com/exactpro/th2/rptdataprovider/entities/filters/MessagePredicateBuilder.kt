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

package com.exactpro.th2.rptdataprovider.entities.filters

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventTypeFilter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.messages.AttachedEventFilters
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageTypeFilter
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import mu.KotlinLogging


class MessagePredicateBuilder(
    requestMap: Map<String, List<String>>,
    private val cradle: CradleService
) : AbstractPredicateBuilder<Message>(requestMap) {

    companion object {
        private val logger = KotlinLogging.logger { }
        val containedFilters = mapOf(
            MessageTypeFilter.filterInfo.name to MessageTypeFilter.filterInfo,
            AttachedEventFilters.filterInfo.name to MessageTypeFilter.filterInfo
        )
    }

    override suspend fun build(): FilterPredicate<Message> {
        val filtersList = mutableListOf<SimpleFilter<Message>>().apply {
            for (filterName in containedFilters.keys) {
                getFilterParameters(filterName)?.let { parameters ->
                    when (filterName) {
                        AttachedEventFilters.filterInfo.name -> {
                            val messagesFromAttachedId =
                                parameters.values.map { cradle.getMessageIdsSuspend(StoredTestEventId(it)) }
                            add(AttachedEventFilters(messagesFromAttachedId, parameters.invert))
                        }
                        MessageTypeFilter.filterInfo.name -> {
                            add(MessageTypeFilter(parameters.values, parameters.invert))
                        }
                        else -> logger.error { "Unknown filter name: $filterName" }
                    }
                }
            }
        }
        return FilterPredicate(filtersList)
    }

    override fun getFiltersNames(): Set<String> {
        return containedFilters.keys
    }

    override fun getFilterInfo(filterName: String): FilterInfo {
        return containedFilters[filterName] ?: throw InvalidRequestException("Incorrect filter name '$filterName'")
    }
}