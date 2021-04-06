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

package com.exactpro.th2.rptdataprovider.entities.filters.info

import com.fasterxml.jackson.annotation.JsonFormat

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
enum class FilterParameterType(val value: String) {
    NUMBER("number"), BOOLEAN("boolean"), STRING("string"), STRING_LIST("string[]");

    override fun toString(): String {
        return value
    }
}

data class Parameter(val name: String, val type: FilterParameterType, val defaultValue: Any?, val hint: String?)

data class FilterInfo(val name: String, val hint: String?, val parameters: List<Parameter>)
