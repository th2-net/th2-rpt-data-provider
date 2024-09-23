/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
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
                    eventCounter.checkCountAndGet(createEventEntity(ProviderEventId(
                        batchId = null,
                        eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                    ))),
                    "root event with unique id",
                )
            },
            {
                assertTrue(
                    eventCounter.checkCountAndGet(createEventEntity(ProviderEventId(
                        batchId = null,
                        eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), rootEventId),
                    ))),
                    "root event with same id",
                )
            },
            {
                assertTrue(
                    eventCounter.checkCountAndGet(
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
                    eventCounter.checkCountAndGet(createEventEntity(
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
                        eventCounter.checkCountAndGet(createEventEntity(ProviderEventId(
                            batchId = null,
                            eventId = StoredTestEventId(BOOK_ID, SCOPE, Instant.now(), NEXT_UUID),
                        ))),
                        "root event with unique id, attempt $it",
                    )
                },
                {
                    assertTrue(
                        eventCounter.checkCountAndGet(createEventEntity(ProviderEventId(
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
                eventCounter.checkCountAndGet(
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
                    eventCounter.checkCountAndGet(
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
                    eventCounter.checkCountAndGet(
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
                eventCounter.checkCountAndGet(
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
                    eventCounter.checkCountAndGet(
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
                    eventCounter.checkCountAndGet(
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

    companion object {
        private val BOOK_ID = BookId("test-book")
        private const val SCOPE = "test-scope"

        private val NEXT_UUID: String
            get() = UUID.randomUUID().toString()

        private fun createEventEntity(
            id: ProviderEventId,
            parentEventId: ProviderEventId? = null,
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
            successful = true,
        )
    }
}