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

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventNameFilter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import mu.KotlinLogging
import java.lang.Exception
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.*
import kotlin.reflect.jvm.javaField

class PredicateFactory<T>(
    private val filters: Map<FilterInfo, (Map<String, List<String>>, CradleService) -> Filter<T>>,
    private val cradleService: CradleService
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private var containedFiltersInfo: Map<String, FilterInfo> = mapOf()

    private var containedFiltersInit: Map<String, (Map<String, List<String>>, CradleService) -> Filter<T>>

    init {
        containedFiltersInfo = filters.entries.associate {
            it.key.name to it.key
        }
        containedFiltersInit = filters.entries.associate {
            it.key.name to it.value
        }
    }

    fun build(requestMap: Map<String, List<String>>): FilterPredicate<T> {
        val filtersList = mutableListOf<Filter<T>>().apply {
            for (filterName in requestMap["filters"] ?: emptyList()) {
                val constructor = containedFiltersInit[filterName]
                    ?: throw InvalidRequestException("Incorrect filter name '$filterName'")
                add(constructor(requestMap, cradleService))
            }
        }
        return FilterPredicate(filtersList)
    }

    fun getFiltersNames(): Set<String> {
        return containedFiltersInfo.keys
    }

    fun getFilterInfo(filterName: String): FilterInfo {
        return containedFiltersInfo[filterName] ?: throw InvalidRequestException("Incorrect filter name '$filterName'")
    }
}