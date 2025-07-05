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

import java.util.concurrent.SynchronousQueue
import java.util.concurrent.atomic.AtomicInteger

private val REQUEST_ID_COUNTER = AtomicInteger(0)
private val REQUEST_ID_QUEUE = SynchronousQueue<Int>()

fun acquireRequestId(): Int {
    return REQUEST_ID_QUEUE.poll() ?: REQUEST_ID_COUNTER.incrementAndGet()
}

fun releaseRequestId(id: Int) {
    REQUEST_ID_QUEUE.put(id)
}

inline fun <T> withRequestId(block: (id: String) -> T): T {
    val id = acquireRequestId()
    try {
        return block.invoke(id.toString())
    } finally {
        releaseRequestId(id)
    }
}