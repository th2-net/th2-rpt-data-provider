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

import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo

abstract class AbstractPredicateBuilder<T>(
    private val requestMap: Map<String, List<String>>
) {
    data class FilterParameters(val invert: Boolean = false, val values: List<String> = listOf())


    protected fun getFilterParameters(filterName: String): FilterParameters? {
        return requestMap["$filterName-values"]?.let {
            FilterParameters(
                invert = requestMap["$filterName-invert"]?.first()?.toBoolean() ?: false,
                values = it
            )
        }
    }

    abstract fun getFiltersNames(): Set<String>

    abstract fun getFilterInfo(filterName: String): FilterInfo

    abstract suspend fun build(): FilterPredicate<T>
}