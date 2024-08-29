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
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.rptdataprovider.ProtoContext
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.internal.StreamName
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

    private val messagesInChunk = 10
    private val chunkCount = 3
    private val batchSize = messagesInChunk * chunkCount

    private val baseStreamName = "test_stream"
    private val streamDirection = "FIRST"

    private val bookId = BookId("")
    private val fullStreamName = "${baseStreamName}:${streamDirection}"
    private val streamNameObject = StreamName(baseStreamName, Direction.valueOf(streamDirection), BookId(""))

    inner class BorderTestParameters(
        val startTimestamp: Instant,
        val endTimestamp: Instant,
        val batchStartTimestamp: Instant,
        val batchEndTimestamp: Instant,
        val resultSize: Int,
        val resultIndexes: List<Long>,
        id: Long? = null
    ) {
        val resumeId = id?.let {
            StoredMessageId(BookId(""), streamNameObject.name, streamNameObject.direction, Instant.now(), it)
        }
    }

    private fun getSearchRequest(
        startTimestamp: Instant,
        endTimestamp: Instant,
        resumeId: StoredMessageId? = null
    ): SseMessageSearchRequest<ProtoRawMessage, Message> {
        val parameters = mutableMapOf(
            "stream" to listOf(fullStreamName),
            "direction" to listOf(streamDirection),
            "startTimestamp" to listOf(startTimestamp.toEpochMilli().toString()),
            "endTimestamp" to listOf(endTimestamp.toEpochMilli().toString()),
            "bookId" to listOf(bookId.name)
        )
        if (resumeId != null) {
            parameters["messageId"] = listOf(resumeId.toString())
        }
        return SseMessageSearchRequest(parameters, FilterPredicate(emptyList()))
    }

    private fun getMessage(timestamp: Instant, globalIndex: AtomicLong? = null): StoredMessage {
        val msg = mockk<StoredMessage>()

        every { msg.timestamp } answers { timestamp }
        var index = 0L
        if (globalIndex != null) {
            index = globalIndex.getAndIncrement()
            every { msg.sequence } answers { index }
        }
        every { msg.sessionAlias } answers { fullStreamName }
        every { msg.protocol } answers { "protocol" }
        every { msg.direction } answers { Direction.FIRST }
        every { msg.content } answers { byteArrayOf(1, 1, 1) }
        every { msg.id } answers {
            StoredMessageId(
                bookId,
                baseStreamName,
                Direction.valueOf(streamDirection),
                timestamp,
                index
            )
        }
        every { msg.metadata } answers { null }
        every { msg.serializedSize } answers { 0 }

        return msg
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun mockContextWithCradleService(batch: StoredMessageBatch): ProtoContext {
        val context: ProtoContext = mockk()

        every { context.configuration.sendEmptyDelay.value } answers { "10" }

        val cradle = mockk<CradleService>()

        coEvery { cradle.getMessagesBatchesSuspend(any()) } answers {
            runBlocking {
                Channel<StoredMessageBatch>(1).also {
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

            repeat(messagesInChunk - 2) {
                add(getMessage(getTimestampBetween(startTimestamp, endTimestamp)))
            }

            add(getMessage(endTimestamp))
        }.let { list ->
            list.sortedBy { it.timestamp }
        }
    }

    private fun chunkedBatch(start: Instant): StoredMessageBatch {
        var index = 1L
        var startTimestamp = start

        val allMessages = mutableListOf<StoredMessage>()
        repeat(chunkCount) {
            val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)

            val messages = getMessages(startTimestamp, endTimestamp)

            messages.forEach { msg ->
                every { msg.sequence } answers { index++ }
                allMessages.add(msg)
            }
            startTimestamp = endTimestamp.plusNanos(1)
        }

        return StoredMessageBatch(allMessages, PageId(BookId("1"), start,"1"), Instant.now())
    }


    @Test
    fun extractByIntervals() {
        var startTimestamp = Instant.parse("2022-04-21T10:00:00Z")

        val batch = chunkedBatch(startTimestamp)

        val context = mockContextWithCradleService(batch)

        var count = 0

        runBlocking {
            repeat(chunkCount + 1) {
                val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)
                val request = getSearchRequest(startTimestamp, endTimestamp)

                val extractor = MessageExtractor(context, request, streamNameObject, this, 1, PipelineStatus())

                var messages: Collection<StoredMessage> = emptyList()

                while (true) {
                    val message = extractor.pollMessage()
                    if (message is PipelineRawBatch)
                        messages = message.storedBatchWrapper.trimmedMessages
                    if (message.streamEmpty) break
                }

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
            assertEquals(batchSize, count)
        }
    }

    private fun getOutOfStartBatch(startTimestamp: Instant, endTimestamp: Instant): StoredMessageBatch {

        val allMessages = mutableListOf<StoredMessage>()
        val index = AtomicLong(1L)

        allMessages.add(getMessage(startTimestamp, index))

        val pivot = getTimestampBetween(startTimestamp, endTimestamp)
        allMessages.add(getMessage(pivot, index))

        allMessages.add(getMessage(endTimestamp, index))

        return StoredMessageBatch(allMessages, PageId(BookId("1"), startTimestamp, "1"), Instant.now())
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

            val extractor = MessageExtractor(context, request, streamNameObject, this, 1, PipelineStatus())

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
}