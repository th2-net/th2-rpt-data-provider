/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.metrics

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.prometheus.client.Gauge
import io.prometheus.client.Summary

private const val METHOD_LABEL = "method"
private const val ID_LABEL = "id"

private val K_LOGGER = KotlinLogging.logger {}


private val ACTIVE_METHOD_GAUGE = Gauge.build(
    "th2_rpt_active_method",
    "This metric provide information about active method"
).labelNames(METHOD_LABEL, ID_LABEL).register()

private val METHOD_EXECUTION_SUMMARY = Summary.build(
    "th2_rpt_method_execution",
    "This metric provide information about method duration"
).labelNames(METHOD_LABEL, ID_LABEL).register()

fun getMethodExecution(method: String, id: String): Summary.Child = METHOD_EXECUTION_SUMMARY.labels(method, id)
fun getActiveMethod(method: String, id: String): Gauge.Child = ACTIVE_METHOD_GAUGE.labels(method, id)

fun handlingLog(message: String, level: Level) = K_LOGGER.at(level) { this.message = "handling $message" }
fun handledLog(timeSec: Double, message: String, level: Level) = K_LOGGER.at(level) { this.message = "handled $message - time=${timeSec}sec" }

inline fun <T> measure(
    method: String,
    id: String,
    description: String = "'$method ($id)' method",
    level: Level = Level.DEBUG,
    block: () -> T
): T {
    val active: Gauge.Child = getActiveMethod(method, id)
    val timer: Summary.Timer = getMethodExecution(method, id).startTimer()
    handlingLog(description, level)
    try {
        active.inc()
        return block.invoke()
    } finally {
        timer.observeDuration().also {
            handledLog(it, description, level)
        }
        active.dec()
    }
}