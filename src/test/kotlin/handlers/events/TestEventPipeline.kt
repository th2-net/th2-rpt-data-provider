/*******************************************************************************
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package handlers.events

import com.exactpro.cradle.Order
import com.exactpro.cradle.testevents.*
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineFilteredMessage
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventStreamPointer
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.MessageStreamPointer
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestEventPipeline {

    data class EventData(val id: StoredTestEventId, val startTimestamp: Instant, val endTimestamp: Instant)

    data class EventsParameters(
        val startTimestamp: Instant,
        val endTimestamp: Instant,
        val resumeId: ProviderEventId?,
        val events: List<List<EventData>>,
        val expectedResult: List<String>
    )

    private fun getSearchRequest(
        startTimestamp: Instant,
        endTimestamp: Instant,
        resumeId: ProviderEventId? = null
    ): SseEventSearchRequest {
        val parameters = mutableMapOf(
            "startTimestamp" to listOf(startTimestamp.toString()),
            "endTimestamp" to listOf(endTimestamp.toString())
        )
        if (resumeId != null) {
            parameters["resumeFromId"] = listOf(resumeId.toString())
        }
        return SseEventSearchRequest(parameters, FilterPredicate(emptyList()))
    }

    private fun mockContextWithCradleService(
        batches: List<StoredTestEventWrapper>,
        resumeId: ProviderEventId?
    ): Context {
        val context: Context = mockk()

        every { context.configuration.sendEmptyDelay.value } answers { "10" }
        every { context.configuration.sseEventSearchStep.value } answers { "10000000000" }
        every { context.configuration.eventSearchChunkSize.value } answers { "1" }
        every { context.configuration.keepAliveTimeout.value } answers { "1" }

        val cradle = mockk<CradleService>()

        coEvery { cradle.getEventsSuspend(any<Instant>(), any<Instant>(), any<Order>()) } answers {
            val start = firstArg<Instant>()
            val end = secondArg<Instant>()
            val order = thirdArg<Order>()
            batches.filter {
                maxInstant(it.startTimestamp, start).isBeforeOrEqual(minInstant(it.endTimestamp, end))
            }.let {
                if (order == Order.DIRECT) it else it.reversed()
            }
        }

        coEvery { cradle.getEventsSuspend(any<StoredTestEventId>(), any<Instant>(), any<Order>()) } answers {
            val start = firstArg<StoredTestEventId>()
            val end = secondArg<Instant>()
            val order = thirdArg<Order>()

            batches.let {
                if (order == Order.DIRECT) it else it.reversed()
            }.dropWhile { batch -> batch.id != start }
        }



        coEvery { cradle.getEventsSuspend(any<Instant>(), any<StoredTestEventId>(), any<Order>()) } answers {
            val start = firstArg<Instant>()
            val end = secondArg<StoredTestEventId>()
            val order = thirdArg<Order>()

            batches.let {
                if (order == Order.DIRECT) it else it.reversed()
            }.dropWhile { batch -> batch.id != end }
        }

        if (resumeId != null) {
            val batch = batches.find { it.asBatch().testEvents.map { it.id }.contains(resumeId.eventId) }
            coEvery { cradle.getEventSuspend(any()) } answers { batch }
        }

        every { context.cradleService } answers { cradle }
        every { context.eventProducer } answers { EventProducer(cradle, ObjectMapper()) }
        return context
    }

    private fun createBatch(batches: List<List<EventData>>): List<StoredTestEventWrapper> {
        return batches.map { events ->
            val storedId = events.first().id
            val batch = StoredTestEventBatch(
                TestEventBatchToStore.builder()
                    .id(StoredTestEventId(storedId.toString().split("-").first()))
                    .parentId(StoredTestEventId("parent"))
                    .build()
            ).also {
                for (event in events) {
                    it.addTestEvent(
                        TestEventToStore.builder()
                            .id(event.id)
                            .name("name")
                            .parentId(it.parentId)
                            .startTimestamp(event.startTimestamp)
                            .content(ByteArray(1))
                            .endTimestamp(event.endTimestamp)
                            .build()
                    )
                }
            }
            StoredTestEventWrapper(batch)
        }
    }

    private fun mockWriter(events: MutableList<EventTreeNode>): StreamWriter {
        return object : StreamWriter {

            override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
                events.add(event)
            }

            override suspend fun write(message: PipelineFilteredMessage, counter: AtomicLong) {
            }

            override suspend fun write(event: Event, lastEventId: AtomicLong) {
            }

            override suspend fun write(streamInfo: List<MessageStreamPointer>) {
            }

            override suspend fun write(streamInfo: EventStreamPointer) {
            }

            override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
            }

            override suspend fun closeWriter() {
            }
        }
    }

    private fun createEvents(batchId: String, startTimestamp: Instant, endTimestamp: Instant): List<EventData> {
        var start = startTimestamp
        var index = 1
        return mutableListOf<EventData>().apply {
            while (start.isBefore(endTimestamp)) {
                val end = start.plus(1, ChronoUnit.MINUTES)
                add(
                    EventData(StoredTestEventId("$batchId-$index"), start, end)
                )
                start = end
                index++
            }
        }
    }

    private fun getIdRange(batchId: String, start: Int, end: Int): List<String> {
        return (start..end).map { "$batchId-$it" }
    }

    private fun baseTestCase(testCase: EventsParameters) {
        val startTimestamp = testCase.startTimestamp
        val endTimestamp = testCase.endTimestamp
        val resumeId = testCase.resumeId

        val request = getSearchRequest(startTimestamp, endTimestamp, resumeId)
        val batch = createBatch(testCase.events)
        val context = mockContextWithCradleService(batch, resumeId)
        val searchEvent = SearchEventsHandler(context)

        val resultEvents = mutableListOf<EventTreeNode>()
        val writer = mockWriter(resultEvents)

        runBlocking {
            searchEvent.searchEventsSse(request, writer)
            coroutineContext.cancelChildren()
        }

        assertArrayEquals(
            testCase.expectedResult.toTypedArray(),
            resultEvents.map { it.id.eventId.toString() }.toTypedArray()
        )
    }

    @Test
    fun `baseTest`() {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")

        val testData = EventsParameters(
            startTimestamp,
            endTimestamp,
            null,
            events = listOf(
                createEvents("1", startTimestamp, startTimestamp.plus(5, ChronoUnit.MINUTES)),
                createEvents(
                    "2",
                    startTimestamp.plus(5, ChronoUnit.MINUTES),
                    startTimestamp.plus(10, ChronoUnit.MINUTES)
                )
            ),
            expectedResult = getIdRange("1", 1, 5) + getIdRange("2", 1, 5)
        )

        baseTestCase(testData)
    }

    @Test
    fun `testIntersectedBatches`() {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")

        val testData = EventsParameters(
            startTimestamp,
            endTimestamp,
            null,
            events = listOf(
                createEvents("1", startTimestamp, startTimestamp.plus(5, ChronoUnit.MINUTES)),
                createEvents(
                    "2",
                    startTimestamp.plus(3, ChronoUnit.MINUTES),
                    startTimestamp.plus(8, ChronoUnit.MINUTES)
                )
            ),
            expectedResult = getIdRange("1", 1, 5) + getIdRange("2", 1, 5)
        )

        baseTestCase(testData)
    }

    @Test
    fun `baseResumeTest`() {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")


        val testData = EventsParameters(
            startTimestamp,
            endTimestamp,
            ProviderEventId(StoredTestEventId("1"), StoredTestEventId("1-4")),
            events = listOf(
                createEvents("1", startTimestamp, startTimestamp.plus(5, ChronoUnit.MINUTES)),
                createEvents(
                    "2",
                    startTimestamp.plus(5, ChronoUnit.MINUTES),
                    startTimestamp.plus(10, ChronoUnit.MINUTES)
                )
            ),
            expectedResult = getIdRange("1", 5, 5) + getIdRange("2", 1, 5)
        )

        baseTestCase(testData)
    }


    @Test
    fun `baseResumeTestSecondBatch`() {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")


        val testData = EventsParameters(
            startTimestamp,
            endTimestamp,
            ProviderEventId(StoredTestEventId("2"), StoredTestEventId("2-1")),
            events = listOf(
                createEvents("1", startTimestamp, startTimestamp.plus(5, ChronoUnit.MINUTES)),
                createEvents(
                    "2",
                    startTimestamp.plus(5, ChronoUnit.MINUTES),
                    startTimestamp.plus(10, ChronoUnit.MINUTES)
                )
            ),
            expectedResult = getIdRange("2", 2, 5)
        )

        baseTestCase(testData)
    }


    @Test
    fun `testIntersectedBatchesResume`() {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")

        val testData = EventsParameters(
            startTimestamp,
            endTimestamp,
            ProviderEventId(StoredTestEventId("1"), StoredTestEventId("1-4")),
            events = listOf(
                createEvents("1", startTimestamp, startTimestamp.plus(5, ChronoUnit.MINUTES)),
                createEvents(
                    "2",
                    startTimestamp.plus(3, ChronoUnit.MINUTES),
                    startTimestamp.plus(8, ChronoUnit.MINUTES)
                )
            ),
            expectedResult = getIdRange("1", 5, 5) + getIdRange("2", 1, 5)
        )

        baseTestCase(testData)
    }
}