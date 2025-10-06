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
package handlers

import com.exactpro.cradle.BookId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.handlers.IParentEventCounter
import com.exactpro.th2.rptdataprovider.handlers.IParentEventCounter.Companion.HASH_MODE
import com.exactpro.th2.rptdataprovider.handlers.IParentEventCounter.Companion.OPTIMIZED_HASH_MODE
import com.exactpro.th2.rptdataprovider.handlers.IParentEventCounter.Companion.OPTIMIZED_MODE
import io.prometheus.client.CollectorRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.UUID

class IParentEventCounterTest {

    @Test
    fun `no limit test`() {
        val eventCounter = IParentEventCounter.create(null)

        val rootEventId = NEXT_UUID
        val parentEventId = ProviderEventId(
            batchId = null,
            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
        )

        assertAll(
            {
                assertTrue(
                    eventCounter.updateCountAndCheck(createEventEntity(ProviderEventId(
                        batchId = null,
                        eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                    ))),
                    "root event with unique id",
                )
            },
            {
                assertTrue(
                    eventCounter.updateCountAndCheck(createEventEntity(ProviderEventId(
                        batchId = null,
                        eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), rootEventId),
                    ))),
                    "root event with same id",
                )
            },
            {
                assertTrue(
                    eventCounter.updateCountAndCheck(
                        createEventEntity(
                            ProviderEventId(
                                batchId = null,
                                eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                            ),
                            parentEventId,
                        )
                    ),
                    "single event id",
                )
            },
            {
                assertTrue(
                    eventCounter.updateCountAndCheck(createEventEntity(
                        ProviderEventId(
                            batchId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                        ),
                        parentEventId,
                    )),
                    "batched event id",
                )
            },
        )
    }

    @Test
    fun `limit root event test`() {
        val limitForParent = 50
        val eventCounter = IParentEventCounter.create(limitForParent.toLong())

        val rootEventId = NEXT_UUID

        repeat(limitForParent * 2) {
            assertAll(
                {
                    assertTrue(
                        eventCounter.updateCountAndCheck(createEventEntity(ProviderEventId(
                            batchId = null,
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                        ))),
                        "root event with unique id, attempt $it",
                    )
                },
                {
                    assertTrue(
                        eventCounter.updateCountAndCheck(createEventEntity(ProviderEventId(
                            batchId = null,
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), rootEventId),
                        ))),
                        "root event with same id, attempt $it",
                    )
                },
            )
        }
    }

    @Test
    fun `singe event test`() {
        val limitForParent = 50
        val eventCounter = IParentEventCounter.create(limitForParent.toLong())

        val parentEventId = ProviderEventId(
            batchId = null,
            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
        )

        repeat(limitForParent) {
            assertTrue(
                eventCounter.updateCountAndCheck(
                    createEventEntity(
                        ProviderEventId(
                            batchId = null,
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                        ),
                        parentEventId,
                    )
                ),
                "single event id, attempt $it",
            )
        }

        val nextEventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)
        assertAll(
            {
                assertFalse(
                    eventCounter.updateCountAndCheck(
                        createEventEntity(
                            ProviderEventId(
                                batchId = null,
                                eventId = nextEventId,
                            ),
                            parentEventId,
                        ),
                    ),
                    "single event id, attempt ${limitForParent + 1}",
                )
            },
            {
                assertFalse(
                    eventCounter.updateCountAndCheck(
                        createEventEntity(
                            ProviderEventId(
                                batchId = null,
                                eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                            ),
                            ProviderEventId(
                                batchId = null,
                                eventId = nextEventId,
                            )
                        ),
                    ),
                    "child of single event id, attempt ${limitForParent + 1}",
                )
            },
        )
    }

    @Test
    fun `batched event test`() {
        val limitForParent = 50
        val eventCounter = IParentEventCounter.create(limitForParent.toLong())

        val parentEventId = ProviderEventId(
            batchId = null,
            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
        )
        val batchId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)

        repeat(limitForParent) {
            assertTrue(
                eventCounter.updateCountAndCheck(
                    createEventEntity(
                        ProviderEventId(
                            batchId = batchId,
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                        ),
                        parentEventId,
                    )
                ),
                "single event id, attempt $it",
            )
        }

        val nextEventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)
        assertAll(
            {
                assertFalse(
                    eventCounter.updateCountAndCheck(
                        createEventEntity(
                            ProviderEventId(
                                batchId = batchId,
                                eventId = nextEventId,
                            ),
                            parentEventId,
                        ),
                    ),
                    "single event id, attempt ${limitForParent + 1}",
                )
            },
            {
                assertFalse(
                    eventCounter.updateCountAndCheck(
                        createEventEntity(
                            ProviderEventId(
                                batchId = batchId,
                                eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                            ),
                            ProviderEventId(
                                batchId = batchId,
                                eventId = nextEventId
                            )
                        ),
                    ),
                    "child of single event id, attempt ${limitForParent + 1}",
                )
            },
        )
    }

    @ParameterizedTest
    @ValueSource(strings = [HASH_MODE, "default"])
    fun `event tree test`(mode: String) {
        IParentEventCounter.create(2, mode).use { eventCounter ->
            val names = mutableSetOf<String>()
            EVENTS.asSequence().forEach {
                println(it)
                if (eventCounter.updateCountAndCheck(createEventEntity(it.id, it.parent))) {
                    names.add(it.name)
                }
            }
    
            assertAll(
                { assertEquals(15.0, getParentEventMetric()) },
                {
                    assertEquals(setOf(
                        "1",
                            "1.1",
                                "1.1.1",
                                    "1.1.1.1",
                        "2", // batch {
                            "2.1",
                                "2.1.1",
                                "2.1.2", // batch }
                        "3", // batch {
                            "3.1",
                                "3.1.1",
                                "3.1.2",
                            "3.2",
                                "3.2.1",
                                "3.2.2", // batch }
                    ), names)
                },
            )
        }
        assertEquals(0.0, getParentEventMetric())
    }

    @ParameterizedTest
    @ValueSource(strings = [OPTIMIZED_MODE, OPTIMIZED_HASH_MODE])
    fun `optimized event tree test`(mode: String) {
        IParentEventCounter.create(2, mode).use { eventCounter ->
            val names = mutableSetOf<String>()
            EVENTS.asSequence().forEach {
                println(it)
                if (eventCounter.updateCountAndCheck(createEventEntity(
                        it.id,
                        it.parent,
                        it.batchParent?.eventId
                    ))) {
                    names.add(it.name)
                }
            }
            
            assertAll(
                { assertEquals(5.0, getParentEventMetric()) },
                {
                    assertEquals(setOf(
                        "1",
                            "1.1",
                                "1.1.1",
                                    "1.1.1.1",
                        "2", // batch {
                            "2.1",
                                "2.1.1",// batch }
                        "3", // batch {
                            "3.1",
                                "3.1.1",// batch }
                    ), names)
                },
            )
        }
        assertEquals(0.0, getParentEventMetric())
    }

    companion object {
        private val BOOK_ID = BookId("test-book")
        private const val SCOPE = "test-scope"

        private val NEXT_UUID: String
            get() = UUID.randomUUID().toString()

        private val EVENTS = eventList {
            root { // from single events
                single {
                    single {
                        single()
                    }
                }
            }
            root { // from single batch
                batch {
                    single {
                        single()
                        single()
                        single()
                    }
                }
            }
            root { // from single batch
                batch {
                    single {
                        single()
                        single()
                        single()
                    }
                    single {
                        single()
                        single()
                        single()
                    }
                    single {
                        single()
                        single()
                        single()
                    }
                }
            }
        }

        private fun createEventEntity(
            id: ProviderEventId,
            parentEventId: ProviderEventId? = null,
            batchParentEventId: StoredTestEventId? = null,
        ) = BaseEventEntity(
            type = "event",
            id = id,
            batchId = id.batchId,
            isBatched = id.batchId != null,
            eventName = "test-event",
            eventType = "test-type",
            startTimestamp = id.eventId.startTimestamp,
            endTimestamp = null,
            parentEventId = parentEventId,
            batchParentEventId = batchParentEventId,
            successful = true,
        )

        private fun getParentEventMetric(): Double = CollectorRegistry.defaultRegistry.getSampleValue("th2_rpt_parent_event_count")
        
        interface Event {
            val name: String
            val id: ProviderEventId
            val parent: ProviderEventId?
            val batchParent: ProviderEventId?
            fun asSequence(): Sequence<Event>
        }

        class EventBatch(
            override val name: String,
            override val parent: ProviderEventId,
        ) : Event {
            private val batchEventId: StoredTestEventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)
            private val children = mutableListOf<BatchedEvent>()

            override val id: ProviderEventId
                get() = TODO("Not yet implemented")

            override val batchParent: ProviderEventId?
                get() = TODO("Not yet implemented")

            init {
                require(parent.batchId == null) {
                    "parent of batch can't be batched"
                }
            }

            fun single(block: BatchedEvent.() -> Unit = {}) {
                BatchedEvent(name = "${name}.${children.size + 1}", parent = parent, batchParent = parent, batchEventId = batchEventId).apply(block).also(children::add)
            }

            override fun asSequence(): Sequence<Event> = children.asSequence().flatMap(Event::asSequence)
        }

        class BatchedEvent(
            override val name: String,
            override val parent: ProviderEventId,
            override val batchParent: ProviderEventId,
            private val batchEventId: StoredTestEventId,
        ) : Event {
            private val eventId: StoredTestEventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)
            private val children = mutableListOf<BatchedEvent>()

            override val id: ProviderEventId
                get() = ProviderEventId(batchEventId, eventId)

            fun single(block: BatchedEvent.() -> Unit = {}) {
                BatchedEvent(name = "$name.${children.size + 1}", parent = id, batchParent = batchParent, batchEventId = batchEventId).apply(block).also(children::add)
            }

            override fun asSequence(): Sequence<Event> = sequenceOf(this) + children.asSequence().flatMap(Event::asSequence)

            override fun toString(): String {
                return buildString {
                    append("name: ").append(name)
                    append(", id: ").append(id.log())
                    append(", parent: ").append(parent.log())
                }
            }
        }

        class SingleEvent(
            override val name: String,
            override val parent: ProviderEventId?
        ) : Event {
            private val children = mutableListOf<Event>()

            override val id: ProviderEventId = ProviderEventId(
                null,
                StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID)
            )
            override val batchParent: ProviderEventId?
                get() = null

            fun single(block: SingleEvent.() -> Unit = {}) {
                SingleEvent(name = "$name.${children.size + 1}", parent = id).apply(block).also(children::add)
            }

            fun batch(block: EventBatch.() -> Unit = {}) {
                EventBatch(name = name, parent = id).apply(block).also(children::add)
            }

            override fun asSequence(): Sequence<Event> = sequenceOf(this) + children.asSequence().flatMap(Event::asSequence)

            override fun toString(): String {
                return buildString {
                    append("name: ").append(name)
                    append(", id: ").append(id.log())
                    parent?.let { append(", parent: ").append(it.log()) }
                }
            }
        }

        class EventList {
            private val roots = mutableListOf<Event>()

            fun root(block: SingleEvent.() -> Unit = {}) {
                SingleEvent(name = (roots.size + 1).toString(), null).apply(block).also(roots::add)
            }

            fun asSequence(): Sequence<Event> = roots.asSequence().flatMap(Event::asSequence)
        }

        fun eventList(block: EventList.() -> Unit): EventList {
            return EventList().apply(block)
        }

        private fun ProviderEventId.log(): String = buildString {
            batchId?.let { append(it.id).append('>') }
            append(eventId.id)
        }
    }
}