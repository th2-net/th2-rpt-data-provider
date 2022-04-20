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

package com.exactpro.th2.rptdataprovider.entities.filters.info

import com.exactpro.th2.dataprovider.grpc.FilterName
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
enum class FilterParameterType(
    val value: String,
    @JsonIgnore
    private val grpcType: com.exactpro.th2.dataprovider.grpc.FilterParameterType
) {
    NUMBER("number", com.exactpro.th2.dataprovider.grpc.FilterParameterType.NUMBER),
    BOOLEAN("boolean", com.exactpro.th2.dataprovider.grpc.FilterParameterType.BOOLEAN),
    STRING("string", com.exactpro.th2.dataprovider.grpc.FilterParameterType.STRING),
    STRING_LIST("string[]", com.exactpro.th2.dataprovider.grpc.FilterParameterType.STRING_LIST);

    override fun toString(): String {
        return value
    }

    @JsonIgnore
    fun toProto(): com.exactpro.th2.dataprovider.grpc.FilterParameterType {
        return grpcType
    }
}

enum class FilterSpecialType {
    ORDINARY, NEED_BODY, NEED_ATTACHED_MESSAGES,
    NEED_ATTACHED_EVENTS, NEED_JSON_BODY, NEED_BODY_BASE64;
}


data class Parameter(val name: String, val type: FilterParameterType, val defaultValue: Any?, val hint: String?) {

    private fun convertAnyToProto(): com.google.protobuf.Any? {
        return defaultValue?.let {
            val message = when (type) {
                FilterParameterType.NUMBER -> com.google.protobuf.Int64Value.newBuilder().setValue(it as Long)
                FilterParameterType.BOOLEAN -> com.google.protobuf.BoolValue.newBuilder()
                    .setValue(it as Boolean)
                FilterParameterType.STRING -> com.google.protobuf.StringValue.newBuilder()
                    .setValue(it as String)
                FilterParameterType.STRING_LIST -> com.google.protobuf.StringValue.newBuilder()
                    .setValue(it.toString())
            }.build()
            com.google.protobuf.Any.pack(message)
        }
    }

    fun convertToProto(): com.exactpro.th2.dataprovider.grpc.FilterParameter {
        return com.exactpro.th2.dataprovider.grpc.FilterParameter.newBuilder()
            .setName(name)
            .setType(type.toProto())
            .also { builder ->
                convertAnyToProto()?.let { builder.setDefaultValue(it) }
                hint?.let { builder.setHint(it) }
            }.build()
    }
}

data class FilterInfo(
    val name: String,
    val hint: String?,
    val parameters: List<Parameter>,
    @JsonIgnore
    val filterSpecialType: FilterSpecialType = FilterSpecialType.ORDINARY
) {
    fun convertToProto(): com.exactpro.th2.dataprovider.grpc.FilterInfoResponse {
        return com.exactpro.th2.dataprovider.grpc.FilterInfoResponse.newBuilder()
            .setName(FilterName.newBuilder().setName(name))
            .addAllParameter(parameters.map { it.convertToProto() })
            .also { builder ->
                hint?.let { builder.setHint(it) }
            }.build()
    }
}
