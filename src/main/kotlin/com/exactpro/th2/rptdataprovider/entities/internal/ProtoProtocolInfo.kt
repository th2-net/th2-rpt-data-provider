/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.rptdataprovider.entities.internal

import io.github.oshai.kotlinlogging.KotlinLogging

object ProtoProtocolInfo {
    val logger = KotlinLogging.logger{}

    private const val TYPE_IMAGE = "image"

    fun isImage(protocolName: String?): Boolean {
        return protocolName?.contains(TYPE_IMAGE) ?: false
    }
}