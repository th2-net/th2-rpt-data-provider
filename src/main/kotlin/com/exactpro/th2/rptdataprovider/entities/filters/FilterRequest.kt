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

import com.exactpro.th2.rptdataprovider.grpc.Filter

interface FilterRequest {
    fun getName(): String
    fun isNegative(): Boolean
    fun getValues(): List<String>?
}

data class HttpFilter(val filterName: String, val requestMap: Map<String, List<String>>) : FilterRequest {
    override fun getName(): String {
        return filterName
    }

    override fun isNegative(): Boolean {
        return requestMap["$filterName-negative"]?.first()?.toBoolean() ?: false
    }

    override fun getValues(): List<String>? {
        return requestMap["$filterName-values"] ?: requestMap["$filterName-value"]
    }
}

data class GrpcFilter(val filterName: String, val filter: Filter) : FilterRequest {
    override fun getName(): String {
        return filterName
    }

    override fun isNegative(): Boolean {
        return if (filter.hasNegative()) {
            filter.negative.value
        } else {
            false
        }
    }

    override fun getValues(): List<String>? {
        return filter.valuesList
    }
}