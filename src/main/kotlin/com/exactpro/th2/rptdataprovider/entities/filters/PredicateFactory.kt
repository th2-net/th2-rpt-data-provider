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

package com.exactpro.th2.rptdataprovider.entities.filters

import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class PredicateFactory<T>(
    filters: Map<FilterInfo, suspend (FilterRequest, CradleService) -> Filter<T>>,
    private val cradleService: CradleService
) {
    private var containedFiltersInfo: Map<String, FilterInfo> = mapOf()

    private var containedFiltersInit: Map<String, suspend (FilterRequest, CradleService) -> Filter<T>>

    init {
        containedFiltersInfo = filters.entries.associate {
            it.key.name to it.key
        }
        containedFiltersInit = filters.entries.associate {
            it.key.name to it.value
        }
    }

    suspend fun build(requestMap: Map<String, List<String>>): FilterPredicate<T> {
        val filtersList = mutableListOf<Filter<T>>().apply {
            for (filterName in requestMap["filters"] ?: emptyList()) {
                val constructor = containedFiltersInit[filterName]
                    ?: throw InvalidRequestException("Incorrect filter name '$filterName'")
                add(constructor(HttpFilter(filterName, requestMap), cradleService))
            }
        }
        return FilterPredicate(filtersList)
    }

    fun getEmptyPredicate(): FilterPredicate<T> {
        return FilterPredicate(emptyList())
    }

    suspend fun build(filters: List<com.exactpro.th2.dataprovider.grpc.Filter>): FilterPredicate<T> {
        val filtersList = mutableListOf<Filter<T>>().apply {
            for (filter in filters) {
                val constructor = containedFiltersInit[filter.name.filterName]
                    ?: throw InvalidRequestException("Incorrect filter name '${filter.name.filterName}'")
                add(constructor(GrpcFilter(filter.name.filterName, filter), cradleService))
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