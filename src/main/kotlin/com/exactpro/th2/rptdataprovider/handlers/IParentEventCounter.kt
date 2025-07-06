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
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import java.security.MessageDigest

internal interface IParentEventCounter : AutoCloseable {
    /**
     * This method use parent event id or event id to limit number of child events.
     * WARNING: event id isn't grantee event unique then this method can't be used for strict limitation.
     * @return false if limit exceeded otherwise true
     */
    fun updateCountAndCheck(event: BaseEventEntity): Boolean

    private object NoLimitedParentEventCounter : IParentEventCounter {
        override fun updateCountAndCheck(event: BaseEventEntity): Boolean = true
        override fun close() { }
    }

    private abstract class LimitedCounter<K>(
        private val limit: Long
    ) : IParentEventCounter {
        private val holder = createHolder()

        protected open val defaultValue: Long?
            get() = null

        override fun close() {
            PARENT_EVENT_COUNTER.dec(holder.size.toDouble())
            holder.clear()
        }

        protected open fun createHolder(): MutableMap<K, Long> = HashMap()

        protected abstract fun String.toKey(): K

        protected fun updateCountAndCheck(key: K): Boolean =
            holder.compute(key) { _, value ->
                if (value == null) {
                    PARENT_EVENT_COUNTER.inc()
                    1L
                } else {
                    val next = value + 1
                    if (value == MAX_EVENT_COUNTER || next > limit) {
                        MAX_EVENT_COUNTER
                    } else {
                        next
                    }
                }
            } != MAX_EVENT_COUNTER

        @Suppress("SameParameterValue")
        protected fun putIfAbsent(key: K, value: Long) {
            if (holder.putIfAbsent(key, value) == defaultValue) {
                PARENT_EVENT_COUNTER.inc()
            }
        }
    }

    private abstract class DefaultLimitedCounter<K>(
        limitForParent: Long
    ) : LimitedCounter<K>(limitForParent) {

        override fun updateCountAndCheck(event: BaseEventEntity): Boolean {
            if (event.parentEventId == null) return true

            return updateCountAndCheck(event.parentEventId.eventId.id.toKey()).also {
                if (!it) putIfAbsent(event.id.eventId.id.toKey(), MAX_EVENT_COUNTER)
            }
        }
    }

    private class LimitedParentEventCounter(
        limitForParent: Long
    ) : DefaultLimitedCounter<String>(limitForParent) {
        override fun String.toKey(): String = this
    }

    private class HashLimitedParentEventCounter(
        limitForParent: Long
    ) : DefaultLimitedCounter<Long>(limitForParent) {
        override val defaultValue: Long
            get() = -1

        override fun createHolder(): MutableMap<Long, Long> = Long2LongOpenHashMap().apply {
            defaultReturnValue(defaultValue)
        }

        override fun String.toKey(): Long = toLongHash()
    }

    private abstract class OptimizedLimitedCounter<K>(
        limitForParent: Long
    ) : LimitedCounter<K>(limitForParent) {

        override fun updateCountAndCheck(event: BaseEventEntity): Boolean {
            if (event.parentEventId == null) return true // exclude root events

            val parentEventId = event.batchParentEventId?.id ?: event.parentEventId.eventId.id

            return updateCountAndCheck(parentEventId.toKey()).also {
                if (!it && !event.isBatched) putIfAbsent(event.id.eventId.id.toKey(), MAX_EVENT_COUNTER)
            }
        }
    }

    private class OptimizedLimitedParentEventCounter(
        limitForParent: Long
    ) : OptimizedLimitedCounter<String>(limitForParent) {
        override fun String.toKey(): String = this
    }

    private class OptimizedHashLimitedParentEventCounter(
        limitForParent: Long
    ) : OptimizedLimitedCounter<Long>(limitForParent) {
        override val defaultValue: Long
            get() = -1

        override fun createHolder(): MutableMap<Long, Long> = Long2LongOpenHashMap().apply {
            defaultReturnValue(defaultValue)
        }

        override fun String.toKey(): Long = toLongHash()
    }

    companion object {
        const val OPTIMIZED_MODE = "optimized"
        const val HASH_MODE = "hash"
        const val OPTIMIZED_HASH_MODE = "optimized-hash"

        private const val MAX_EVENT_COUNTER = Long.MAX_VALUE
        private val PARENT_EVENT_COUNTER = Gauge
            .build("th2_rpt_parent_event_count", "Number of parent events are cached in memory")
            .register()

        private val MESSAGE_DIGEST = ThreadLocal.withInitial { MessageDigest.getInstance("MD5") }

        private fun String.toLongHash(): Long {
            val hashBytes = MESSAGE_DIGEST.get().digest(this.toByteArray())

            var longHash: Long = 0
            for (i in 0 until 8) {
                longHash = (longHash shl 8) or (hashBytes[i].toLong() and 0xFF)
            }
            return longHash
        }

        fun create(limitForParent: Long? = null, mode: String = "default"): IParentEventCounter =
            limitForParent?.let {
                when(mode) {
                    OPTIMIZED_MODE -> OptimizedLimitedParentEventCounter(it)
                    HASH_MODE -> HashLimitedParentEventCounter(it)
                    OPTIMIZED_HASH_MODE -> OptimizedHashLimitedParentEventCounter(it)
                    else -> LimitedParentEventCounter(it)
                }
            } ?: NoLimitedParentEventCounter
    }
}