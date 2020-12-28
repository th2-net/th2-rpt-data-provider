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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

class SessionToPinConverter(initMap: Map<String, List<String>>) {
    private val sessionToPinMap: Map<String, String> = initMap.flatMap { entity ->
        entity.value.map { it to entity.key }
    }.associate { it }

    private val DEFAULT_PIN = "to_codec"

    fun getPin(sessionName: String?): String {
        return sessionName?.let { sessionToPinMap[sessionName] } ?: DEFAULT_PIN
    }
}