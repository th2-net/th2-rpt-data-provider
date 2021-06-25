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

package com.exactpro.th2.rptdataprovider.services.cradle

import com.datastax.oss.driver.api.core.DriverTimeoutException
import kotlinx.coroutines.delay
import mu.KotlinLogging

private val logger = KotlinLogging.logger { }

suspend fun <T> databaseRequestRetry(dbRetryDelay: Long, request: suspend () -> Iterable<T>): Iterable<T> {
    var goodRequest = false
    var result: Iterable<T> = emptyList()
    while (!goodRequest) {
        try {
            result = request.invoke()
            goodRequest = true
        } catch (e: DriverTimeoutException) {
            logger.debug { "try to reconnect" }
            delay(dbRetryDelay)
        }
    }
    return result
}