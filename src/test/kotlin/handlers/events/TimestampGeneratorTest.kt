/*******************************************************************************
 * Copyright (c) 2022-2022, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package handlers.events

import com.exactpro.cradle.BookId
import com.exactpro.cradle.PageId
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.th2.rptdataprovider.*
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.events.SearchInterval
import com.exactpro.th2.rptdataprovider.handlers.events.TimeIntervalGenerator
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.time.temporal.ChronoUnit


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TimestampGeneratorTest {

    private val startTimestamp = Instant.parse("2022-04-21T12:00:00Z")
    private val endTimestamp = Instant.parse("2022-04-21T23:00:00Z")

    private val bookId = BookId("testBook")
    private val scope = "testScope"

    private val eventId = StoredTestEventId(bookId, scope, startTimestamp, "id")
    private val providerEventIdSingle = ProviderEventId(null, eventId)


    private fun getSearchRequest(
        startTimestamp: Instant?,
        endTimestamp: Instant?,
        searchDirection: TimeRelation = TimeRelation.AFTER,
        resumeId: ProviderEventId? = null
    ): SseEventSearchRequest {
        val parameters = mutableMapOf<String, List<String>>()

        if (startTimestamp != null) {
            parameters["startTimestamp"] = listOf(startTimestamp.toEpochMilli().toString())
        }

        if (endTimestamp != null) {
            parameters["endTimestamp"] = listOf(endTimestamp.toEpochMilli().toString())
        }

        if (resumeId != null) {
            parameters["resumeFromId"] = listOf(resumeId.toString())
        }

        return SseEventSearchRequest(parameters, FilterPredicate(emptyList()))
            .copy(searchDirection = searchDirection)
            .also {
                it.checkRequest()
            }
    }


    private fun mockEvent(startTimestamp: Instant, resumeId: ProviderEventId): StoredTestEvent {
        val event: StoredTestEvent = mockk()

        every { event.startTimestamp } answers { startTimestamp }

        every { event.id } answers { resumeId.eventId }

        return event
    }


    @Test
    fun `testTimestampInOneDay`() {
        val request = getSearchRequest(startTimestamp, endTimestamp)
        val generator = TimeIntervalGenerator(request, null, null)
        val result = generator.toList()

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = startTimestamp, endInterval = endTimestamp,
                    startRequest = startTimestamp, endRequest = endTimestamp,
                    providerResumeId = null
                )
            ),
            result
        )
    }

    @Test
    fun `testTimestampInOneDayReverse`() {
        val request = getSearchRequest(endTimestamp, startTimestamp, TimeRelation.BEFORE)
        val generator = TimeIntervalGenerator(request, null, null)
        val result = generator.toList()

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = startTimestamp, endInterval = endTimestamp,
                    startRequest = endTimestamp, endRequest = startTimestamp,
                    providerResumeId = null
                )
            ),
            result
        )
    }

    @Test
    fun `testTimestampInDifferentDays`() {
        val startPlusOneDay = startTimestamp.plus(1, ChronoUnit.DAYS)

        val request = getSearchRequest(startTimestamp, startPlusOneDay)
        val generator = TimeIntervalGenerator(request, null, null)
        val result = generator.toList()

        val startNextDay = getDayStart(startPlusOneDay)

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = startTimestamp, endInterval = startNextDay.minusNanos(1).toInstant(),
                    startRequest = startTimestamp, endRequest = startPlusOneDay,
                    providerResumeId = null
                ),
                SearchInterval(
                    startInterval = startNextDay.toInstant(), endInterval = startPlusOneDay,
                    startRequest = startTimestamp, endRequest = startPlusOneDay,
                    providerResumeId = null
                )
            ),
            result
        )
    }


    @Test
    fun `testTimestampInDifferentDaysReverse`() {
        val startMinusOneDay = startTimestamp.minus(1, ChronoUnit.DAYS)

        val request = getSearchRequest(startTimestamp, startMinusOneDay, TimeRelation.BEFORE)
        val generator = TimeIntervalGenerator(request, null, null)
        val result = generator.toList()

        val startNextDay = getDayStart(startTimestamp)

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = startNextDay.toInstant(), endInterval = startTimestamp,
                    startRequest = startTimestamp, endRequest = startMinusOneDay,
                    providerResumeId = null
                ),
                SearchInterval(
                    startInterval = startMinusOneDay, endInterval = startNextDay.minusNanos(1).toInstant(),
                    startRequest = startTimestamp, endRequest = startMinusOneDay,
                    providerResumeId = null
                )
            ),
            result
        )
    }

    @Test
    fun `testResumeIdSingleInOneDay`() {
        val request = getSearchRequest(startTimestamp, endTimestamp, resumeId = providerEventIdSingle)

        val eventStart = startTimestamp.plusSeconds(10)

        val event = mockEvent(eventStart, providerEventIdSingle)

        val generator = TimeIntervalGenerator(request, providerEventIdSingle, event)
        val result = generator.toList()

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = eventStart, endInterval = endTimestamp,
                    startRequest = startTimestamp, endRequest = endTimestamp,
                    providerResumeId = providerEventIdSingle
                )
            ),
            result
        )
    }


    @Test
    fun `testResumeIdSingleInTwoDays`() {
        val startPlusOneDay = startTimestamp.plus(1, ChronoUnit.DAYS)

        val request = getSearchRequest(null, startPlusOneDay, resumeId = providerEventIdSingle)

        val eventStart = startTimestamp.plusSeconds(10)
        val event = mockEvent(eventStart, providerEventIdSingle)

        val generator = TimeIntervalGenerator(request, providerEventIdSingle, event)
        val result = generator.toList()

        val startNextDay = getDayStart(startPlusOneDay)

        Assertions.assertEquals(
            listOf(
                SearchInterval(
                    startInterval = eventStart, endInterval = startNextDay.minusNanos(1).toInstant(),
                    startRequest = eventStart, endRequest = startPlusOneDay,
                    providerResumeId = providerEventIdSingle
                ),
                SearchInterval(
                    startInterval = startNextDay.toInstant(), endInterval = startPlusOneDay,
                    startRequest = eventStart, endRequest = startPlusOneDay,
                    providerResumeId = null
                )
            ),
            result
        )
    }

}