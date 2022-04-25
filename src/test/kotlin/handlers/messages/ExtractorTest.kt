package handlers.messages

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageExtractor
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Timestamp


import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit
import java.util.Collections
import java.util.Random
import kotlin.math.min

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExtractorTest {

    private lateinit var context: Context
    private lateinit var batch: StoredMessageBatch

    private val messagesInChunk = 10
    private val chunkCount = 3
    private val batchSize = messagesInChunk * chunkCount

    private fun getMessage(streamName: String, timestamp: Instant): MessageToStore {
        val msg = mockk<MessageToStore>()

        every { msg.timestamp } answers { timestamp }

        every { msg.streamName } answers { streamName }
        every { msg.direction } answers { Direction.FIRST }
        every { msg.getContent() } answers { byteArrayOf(1, 1, 1) }
        every { msg.metadata } answers { null }

        return msg
    }

    private fun getMessages(streamName: String, startTimestamp: Instant, endTimestamp: Instant): List<MessageToStore> {
        return mutableListOf<MessageToStore>().apply {
            add(getMessage(streamName, startTimestamp))
            repeat(messagesInChunk - 2) {
                val diff = kotlin.random.Random.nextLong(
                    startTimestamp.toEpochMilli() + 1000,
                    endTimestamp.toEpochMilli() - 1000
                )
                add(getMessage(streamName, Instant.ofEpochMilli(diff)))
            }
            add(getMessage(streamName, endTimestamp))
        }.let { list ->
            list.sortedBy { it.timestamp }
        }
    }

    private fun getBatch(streamName: String): StoredMessageBatch {
        var index = 1L
        var startTimestamp = Instant.parse("2022-04-21T10:00:00Z")

        return StoredMessageBatch().also { batch ->
            repeat(chunkCount) {

                val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)

                val messages = getMessages(streamName, startTimestamp, endTimestamp)

                messages.forEach { msg ->
                    every { msg.index } answers { index++ }
                    batch.addMessage(msg)
                }
                startTimestamp = endTimestamp.plusNanos(1)
            }
        }
    }

    @BeforeAll
    fun prepareExtractor() {
        context = mockk()
        every { context.configuration.sendEmptyDelay.value } answers { "1000" }
        every { context.configuration.sendPipelineStatus.value } answers { "false" }

        val cradle = mockk<CradleService>()

        coEvery { cradle.getMessagesBatchesSuspend(any()) } answers {
            runBlocking {
                Channel<StoredMessageBatch>(1).also {
                    GlobalScope.launch {
                        batch = getBatch("test_stream")
                        it.send(batch)
                        it.close()
                    }
                }
            }
        }

        every { context.cradleService } answers { cradle }
    }

    private fun getSearchRequest(startTimestamp: Instant, endTimestamp: Instant): SseMessageSearchRequest {
        val parameters = mapOf(
            "stream" to listOf("test_stream:first"),
            "direction" to listOf("next"),
            "startTimestamp" to listOf(startTimestamp.toEpochMilli().toString()),
            "endTimestamp" to listOf(endTimestamp.toEpochMilli().toString())
        )
        return SseMessageSearchRequest(parameters, FilterPredicate(emptyList()))
    }

    @Test
    fun `extractByIntervals`() {

        val streamName = StreamName("test_stream", Direction.FIRST)

        var startTimestamp = Instant.parse("2022-04-21T10:00:00Z")

        var count = 0

        runBlocking {
            repeat(chunkCount + 1) {
                val endTimestamp = startTimestamp.plus(1, ChronoUnit.HOURS)
                val request = getSearchRequest(startTimestamp, endTimestamp)

                val extractor = MessageExtractor(
                    context, request, streamName,
                    this, 1, PipelineStatus(context)
                )

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
}