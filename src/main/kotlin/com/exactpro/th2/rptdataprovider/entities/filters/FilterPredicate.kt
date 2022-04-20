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

package com.exactpro.th2.rptdataprovider.entities.filters

import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterSpecialType
import com.exactpro.th2.rptdataprovider.logTime
import mu.KotlinLogging
import kotlin.system.measureTimeMillis

class FilterPredicate<T>(private val filters: List<Filter<T>>, private val specialTypes: List<FilterSpecialType>) {
    companion object {
        val logger = KotlinLogging.logger {  }
    }

    constructor(filters: List<Filter<T>>) : this(
        filters = filters,
        specialTypes = filters.map { it.getInfo().filterSpecialType }
    )

    fun apply(element: T): Boolean {
        var result = false
        measureTimeMillis {
            result = isEmpty()
            result = filters.all { it.match(element) }
        }.also { logger.trace { "applying filters $it ms" } }
        return result
    }


    fun getSpecialTypes(): List<FilterSpecialType> {
        return specialTypes
    }

    fun isEmpty(): Boolean {
        return filters.isEmpty()
    }
}