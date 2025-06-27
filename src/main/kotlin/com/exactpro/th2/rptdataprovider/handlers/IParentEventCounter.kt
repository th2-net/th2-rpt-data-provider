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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

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
        private val parentEventCounter = ConcurrentHashMap<String, AtomicLong>()

        override fun checkCountAndGet(event: BaseEventEntity): Boolean {
            if (event.parentEventId == null) {
                return true
            }

            val value = parentEventCounter.compute(event.parentEventId.eventId.id) { _, value ->
                if (value == null) {
                    AtomicLong(1)
                } else {
                    if (value === MAX_EVENT_COUNTER) {
                        parentEventCounter.putIfAbsent(event.id.eventId.id, MAX_EVENT_COUNTER)
                        MAX_EVENT_COUNTER
                    } else {
                        if (value.incrementAndGet() > limitForParent) {
                            parentEventCounter.putIfAbsent(event.id.eventId.id, MAX_EVENT_COUNTER)
                            MAX_EVENT_COUNTER
                        } else {
                            value
                        }
                    }
                }
            }

            return value !== MAX_EVENT_COUNTER
        }
    }

    companion object {
        private val MAX_EVENT_COUNTER = AtomicLong(Long.MAX_VALUE)

        fun create(limitForParent: Long? = null, ignoreLimitForParent: Boolean = false): IParentEventCounter = if (ignoreLimitForParent || limitForParent == null) {
            NoLimitedParentEventCounter
        } else {
            LimitedParentEventCounter(limitForParent)
        }
    }
}