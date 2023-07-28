/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.responses.BodyHttpMessage
import com.exactpro.th2.rptdataprovider.entities.responses.BodyHttpSubMessage
import com.exactpro.th2.rptdataprovider.entities.responses.HttpBodyWrapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object MessageMapper {
    private val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .registerModule(
            KotlinModule.Builder()
                .withReflectionCacheSize(512)
                .configure(KotlinFeature.NullToEmptyCollection, false)
                .configure(KotlinFeature.NullToEmptyMap, false)
                .configure(KotlinFeature.NullIsSameAsDefault, false)
                .configure(KotlinFeature.SingletonSupport, false)
                .configure(KotlinFeature.StrictNullChecks, false)
                .
                build()
        )


    internal fun getBodyMessage(messageWithMetadata: MessageWithMetadata<*, *>): BodyHttpMessage? {
        return with(messageWithMetadata) {
            val body = message.parsedMessageGroup?.let {
                it.zip(filteredBody).map { (msg, filtered) ->
                    HttpBodyWrapper.from(msg, filtered)
                }
            }
            val convertedBody = if (body != null) {
                if (body.size == 1) {
                    jacksonMapper.readValue(body[0].jsonMessage, BodyHttpMessage::class.java)
                } else {
                    mergeFieldsHttp(body)
                }
            } else null
            convertedBody
        }
    }

    private fun mergeFieldsHttp(body: List<HttpBodyWrapper>): BodyHttpMessage {
        val res = jacksonMapper.readValue(body[0].jsonMessage, BodyHttpMessage::class.java)
        res.fields = emptyMap<String, Any>().toMutableMap()
        body.map {
            it.subsequenceId.joinToString("-") to it
        }
            .map {
                "${it.second.messageType}-${it.first}" to jacksonMapper.readValue(
                    it.second.jsonMessage, BodyHttpMessage::class.java
                )
            }
            .forEach {
                res.fields?.set(
                    it.first, BodyHttpSubMessage(
                        mutableMapOf("fields" to (it.second.fields ?: mutableMapOf()))
                    )
                )
            }
        return res
    }
}
