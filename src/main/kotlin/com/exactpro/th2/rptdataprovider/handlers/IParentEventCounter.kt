/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import io.prometheus.client.Gauge
import java.lang.ref.Cleaner
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

internal interface IParentEventCounter {
    /**
     * This method use parent event id or event id to limit number of child events.
     * WARNING: event id isn't grantee event unique then this method can't be used for strict limitation.
     * @return false if limit exceeded otherwise true
     */
    fun checkCountAndGet(event: BaseEventEntity): Boolean

    private object NoLimitedParentEventCounter : IParentEventCounter {
        override fun checkCountAndGet(event: BaseEventEntity): Boolean = true
    }

    private class LimitedParentEventCounter(
        private val limitForParent: Long
    ) : IParentEventCounter {
        private val parentEventCounter = ConcurrentHashMap<String, Long>()

        init {
            CLEANER.register(this) {
                PARENT_EVENT_COUNTER.dec(parentEventCounter.size.toDouble())
            }
        }

        override fun checkCountAndGet(event: BaseEventEntity): Boolean {
            if (event.parentEventId == null) return true // exclude root events
            if (event.parentEventId.batchId != null) return true // exclude parents inside batch

            return parentEventCounter.compute(event.parentEventId.eventId.id) { _, value ->
                if (value == null) {
                    PARENT_EVENT_COUNTER.inc()
                    1L
                } else {
                    val next = value + 1
                    if (value == MAX_EVENT_COUNTER || next > limitForParent) {
                        if (!event.isBatched) { // exclude batched events
                            if (parentEventCounter.putIfAbsent(event.id.eventId.id, MAX_EVENT_COUNTER) == null) {
                                PARENT_EVENT_COUNTER.inc()
                            }
                        }
                        MAX_EVENT_COUNTER
                    } else {
                        next
                    }
                }
            } != MAX_EVENT_COUNTER
        }
    }

    private class HashParentEventCounter(
        private val limitForParent: Long
    ) : IParentEventCounter {
        private val parentEventCounter = ConcurrentHashMap<Long, Long>()

        init {
            CLEANER.register(this) {
                PARENT_EVENT_COUNTER.dec(parentEventCounter.size.toDouble())
            }
        }

        override fun checkCountAndGet(event: BaseEventEntity): Boolean {
            if (event.parentEventId == null) return true // exclude root events
            if (event.parentEventId.batchId != null) return true // exclude parents inside batch

            return parentEventCounter.compute(event.parentEventId.eventId.id.toLongHash()) { _, value ->
                if (value == null) {
                    PARENT_EVENT_COUNTER.inc()
                    1L
                } else {
                    val next = value + 1
                    if (value == MAX_EVENT_COUNTER || next > limitForParent) {
                        if (!event.isBatched) { // exclude batched events
                            if (parentEventCounter.putIfAbsent(event.id.eventId.id.toLongHash(), MAX_EVENT_COUNTER) == null) {
                                PARENT_EVENT_COUNTER.inc()
                            }
                        }
                        MAX_EVENT_COUNTER
                    } else {
                        next
                    }
                }
            } != MAX_EVENT_COUNTER
        }

        companion object {
            private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("MD5") }

            fun String.toLongHash(): Long {
                val hashBytes = messageDigest.get().digest(this.toByteArray())

                var longHash: Long = 0
                for (i in 0 until 8) {
                    longHash = (longHash shl 8) or (hashBytes[i].toLong() and 0xFF)
                }
                return longHash
            }
        }
    }

    companion object {
        private val PARENT_EVENT_COUNTER = Gauge
            .build("th2_rpt_parent_event_count", "Number of parent events are cached in memory")
            .register()

        private val CLEANER = Cleaner.create()

        private const val MAX_EVENT_COUNTER = Long.MAX_VALUE

        fun create(limitForParent: Long? = null, mode: String = "limit"): IParentEventCounter = if (limitForParent == null) {
            NoLimitedParentEventCounter
        } else {
            when(mode) {
                "hash" -> HashParentEventCounter(limitForParent)
                else -> LimitedParentEventCounter(limitForParent)
            }
        }
    }
}