/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package handlers.messages

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.PageId
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.rptdataprovider.ProtoContext
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageExtractor
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExtractorTest {
    inner class BorderTestParameters(
        val startTimestamp: Instant,
        val endTimestamp: Instant,
        val batchStartTimestamp: Instant,
        val batchEndTimestamp: Instant,
        val resultSize: Int,
        val resultIndexes: List<Long>,
        id: Long? = null
    ) {
        val resumeId = id?.let { StoredMessageId(BOOK_ID, STREAM_NAME_OBJECT.name, Direction.FIRST, Instant.now(), it) }
    }

    private fun getSearchRequest(
        startTimestamp: Instant,
        endTimestamp: Instant,
        resumeId: StoredMessageId? = null
    ): SseMessageSearchRequest<ProtoRawMessage, Message> {
        val parameters = mutableMapOf(
            "stream" to listOf(STREAM_NAME),
            "startTimestamp" to listOf(startTimestamp.toEpochMilli().toString()),
            "endTimestamp" to listOf(endTimestamp.toEpochMilli().toString()),
            "bookId" to listOf(BOOK_ID.name)
        )
        if (resumeId != null) {
            parameters["messageId"] = listOf(resumeId.toString())
        }
        return SseMessageSearchRequest(parameters, FilterPredicate(emptyList()))
    }

    private fun getMessage(timestamp: Instant, globalIndex: AtomicLong? = null, direction: Direction = Direction.FIRST): StoredMessage {
        val msg = mockk<StoredMessage>()

        every { msg.timestamp } answers { timestamp }
        var index = 0L
        if (globalIndex != null) {
            index = globalIndex.getAndIncrement()
            every { msg.sequence } answers { index }
        }
        every { msg.sessionAlias } answers { STREAM_NAME }
        every { msg.pageId } answers { PageId(BOOK_ID, Instant.MIN, "page_1") }
        every { msg.protocol } answers { "protocol" }
        every { msg.direction } answers { direction }
        every { msg.content } answers { byteArrayOf(1, 1, 1) }
        every { msg.id } answers {
            StoredMessageId(
                BOOK_ID,
                STREAM_NAME,
                direction,
                timestamp,
                index
            )
        }
        every { msg.metadata } answers { null }
        every { msg.serializedSize } answers { 1 }

        return msg
    }

    private fun getMessages(
        sequencePattern: List<Direction>,
        startTimestamp: Instant
    ): List<StoredMessage> {
        val indexFirst = AtomicLong(1)
        val indexSecond = AtomicLong(1)

        return sequencePattern.map { direction ->
            getMessage(
                startTimestamp,
                if (direction == Direction.FIRST) indexFirst else indexSecond,
                direction
            )
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun mockContextWithCradleService(batch: StoredGroupedMessageBatch): ProtoContext {
        val context: ProtoContext = mockk()

        every { context.configuration.sendEmptyDelay.value } answers { "10" }

        val cradle = mockk<CradleService>()
        coEvery {
            cradle.getSessionGroup(any(), any(), any(), any())
        } answers { SESSION_GROUP }

        coEvery { cradle.getGroupedMessages(any(), any()) } answers {
            runBlocking {
                Channel<StoredGroupedMessageBatch>(1).also {
                    GlobalScope.launch {
                        it.send(batch)
                        it.close()
                    }
                }
            }
        }

        every { context.cradleService } answers { cradle }

        return context
    }

    private fun getTimestampBetween(startTimestamp: Instant, endTimestamp: Instant): Instant {
        val diff = Random.nextLong(startTimestamp.toEpochMilli() + 10, endTimestamp.toEpochMilli() - 10)
        return Instant.ofEpochMilli(diff)
    }

    private fun getMessages(startTimestamp: Instant, endTimestamp: Instant): List<StoredMessage> {
        return mutableListOf<StoredMessage>().apply {
            add(getMessage(startTimestamp))

            repeat(MESSAGES_IN_CHUNK - 2) {
                add(getMessage(getTimestampBetween(startTimestamp, endTimestamp)))
            }

            add(getMessage(endTimestamp))
        }.let { list ->
            list.sortedBy { it.timestamp }
        }
    }

    private fun chunkedGroupedBatch(start: Instant): StoredGroupedMessageBatch {
        var index = 1L
        var startTimestamp = start

        val allMessages = mutableListOf<StoredMessage>()
        repeat(CHUNK_COUNT) {
            val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)

            val messages = getMessages(startTimestamp, endTimestamp)

            messages.forEach { msg ->
                every { msg.sequence } answers { index++ }
                allMessages.add(msg)
            }
            startTimestamp = endTimestamp.plusNanos(1)
        }

        return StoredGroupedMessageBatch(SESSION_GROUP, allMessages, PageId(BookId("1"), start,"1"), Instant.now())
    }

    @Test
    fun extractByIntervals() {
        var startTimestamp = Instant.parse("2022-04-21T10:00:00Z")
        val batch = chunkedGroupedBatch(startTimestamp)
        val context = mockContextWithCradleService(batch)
        var count = 0

        runBlocking {
            repeat(CHUNK_COUNT + 1) {
                val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)
                val request = getSearchRequest(startTimestamp, endTimestamp)

                val extractor = MessageExtractor(context, request, STREAM_NAME_OBJECT, this, 1, PipelineStatus())

                var messages: Collection<StoredMessage> = emptyList()
                do {
                    val message = extractor.pollMessage()
                    if (message is PipelineRawBatch)
                        messages = message.storedBatchWrapper.trimmedMessages
                } while (!message.streamEmpty)

                count += messages.size

                val messagesInTimeRange = batch.messages.filter {
                    it.timestamp.isAfterOrEqual(startTimestamp) && it.timestamp.isBefore(endTimestamp)
                }

                assertArrayEquals(
                    messagesInTimeRange.map { it.timestamp }.toTypedArray(),
                    messages.map { it.timestamp }.toTypedArray()
                )

                if (messages.isEmpty())
                    return@repeat

                startTimestamp = endTimestamp
            }
            coroutineContext.cancelChildren()
            assertEquals(BATCH_SIZE, count)
        }
    }

    @Test
    fun resumeExtract() {
        val resumeIndex = 4
        val startTimestamp = Instant.parse("2022-04-21T10:00:00Z")
        val messages = getMessages(
            listOf(Direction.SECOND, Direction.SECOND, Direction.FIRST, Direction.SECOND, Direction.FIRST, Direction.FIRST),
            startTimestamp
        )
        val batch = StoredGroupedMessageBatch(SESSION_GROUP, messages, PageId(BookId("1"), startTimestamp, "1"), Instant.now())
        val context = mockContextWithCradleService(batch)

        val request = getSearchRequest(
            startTimestamp,
            startTimestamp.plus(1, ChronoUnit.MINUTES),
            messages[resumeIndex].id
        )

        val extractedMessages: List<StoredMessage>
        runBlocking {
            val extractor = MessageExtractor(context, request, STREAM_NAME_OBJECT, this, 1, PipelineStatus())
            var extractedMessagesCollection: Collection<StoredMessage> = emptyList()
            do {
                val message = extractor.pollMessage()
                if (message is PipelineRawBatch)
                    extractedMessagesCollection = message.storedBatchWrapper.trimmedMessages
            } while (!message.streamEmpty)
            extractedMessages = ArrayList(extractedMessagesCollection)
            coroutineContext.cancelChildren()
        }

        assertEquals(messages.size - resumeIndex, extractedMessages.size)
        assertEquals(messages[resumeIndex].id, extractedMessages[0].id)
        assertEquals(messages[resumeIndex + 1].id, extractedMessages[1].id)
    }

    private fun getOutOfStartBatch(startTimestamp: Instant, endTimestamp: Instant): StoredGroupedMessageBatch {
        val allMessages = mutableListOf<StoredMessage>()
        val index = AtomicLong(1L)

        allMessages.add(getMessage(startTimestamp, index))

        val pivot = getTimestampBetween(startTimestamp, endTimestamp)
        allMessages.add(getMessage(pivot, index))

        allMessages.add(getMessage(endTimestamp, index))

        return StoredGroupedMessageBatch(SESSION_GROUP, allMessages, PageId(BookId("1"), startTimestamp, "1"), Instant.now())
    }

    private fun testBorders(
        startTimestamp: Instant,
        endTimestamp: Instant,
        batchStartTimestamp: Instant,
        batchEndTimestamp: Instant,
        resumeId: StoredMessageId? = null
    ): List<StoredMessage> {
        val batch = getOutOfStartBatch(batchStartTimestamp, batchEndTimestamp)
        val context = mockContextWithCradleService(batch)

        val resultMessages = mutableListOf<StoredMessage>()

        runBlocking {
            val request = getSearchRequest(startTimestamp, endTimestamp, resumeId)

            val extractor = MessageExtractor(context, request, STREAM_NAME_OBJECT, this, 1, PipelineStatus())

            do {
                val message = extractor.pollMessage()
                if (message is PipelineRawBatch) {
                    resultMessages.addAll(message.storedBatchWrapper.trimmedMessages)
                }
            } while (!message.streamEmpty)

            coroutineContext.cancelChildren()
        }

        return resultMessages
    }

    private fun provideExtractorCase(): Iterable<Arguments> {
        val start = Instant.parse("2022-04-21T00:00:00Z")
        val end = Instant.parse("2022-04-21T01:00:00Z")
        return listOf(
            BorderTestParameters(start, end, start, end, 2, listOf(1L, 2L)),
            BorderTestParameters(start, end.plusMillis(1), start, end, 3, listOf(1L, 2L, 3L)),
            BorderTestParameters(start.plusMillis(1), end, start, end, 1, listOf(2L)),
            BorderTestParameters(start, end, start, end, 2, listOf(1L, 2L), id = 1),
            BorderTestParameters(start, end, start, end, 1, listOf(2L), id = 2),
            BorderTestParameters(start, end.plusMillis(1), start, end, 2, listOf(2L, 3L), id = 2)
        ).map { Arguments { listOf(it).toTypedArray() } }
    }

    @ParameterizedTest
    @MethodSource("provideExtractorCase")
    fun bordersTest(testParameters: BorderTestParameters) {
        val resultMessages = testBorders(
            testParameters.startTimestamp,
            testParameters.endTimestamp,
            testParameters.batchStartTimestamp,
            testParameters.batchEndTimestamp,
            testParameters.resumeId
        )

        assertEquals(testParameters.resultSize, resultMessages.size)
        assertArrayEquals(
            testParameters.resultIndexes.toTypedArray(),
            resultMessages.map { it.sequence }.toTypedArray()
        )
    }

    companion object {
        private const val MESSAGES_IN_CHUNK = 10
        private const val CHUNK_COUNT = 3
        private const val BATCH_SIZE = MESSAGES_IN_CHUNK * CHUNK_COUNT
        private const val STREAM_NAME = "test_stream"
        private val BOOK_ID = BookId("test_book_01")
        private const val SESSION_GROUP = "session_group_1"
        private val STREAM_NAME_OBJECT = CommonStreamName(BOOK_ID, STREAM_NAME)
    }
}