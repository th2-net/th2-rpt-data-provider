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

package handlers.messages


import com.exactpro.cradle.Direction
<<<<<<< HEAD
import com.exactpro.cradle.messages.MessageToStore
=======
>>>>>>> 1e98c1b5... fix events search resume from id
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageStreamPointer
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import io.mockk.every
import io.mockk.mockk
<<<<<<< HEAD
import kotlinx.coroutines.*
=======
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
>>>>>>> 1e98c1b5... fix events search resume from id
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MergerTest {
    private val baseStreamName = "test_stream"

    private val streamDirection = listOf("first", "second")

    private val fullStreamName = streamDirection.map { "${baseStreamName}:${it}" }

    private val streamNameObjects = streamDirection.map {
        StreamName(baseStreamName, Direction.byLabel(it))
    }

    private val direction = "next"

    data class StreamInfoCase(
        val startTimestamp: Instant,
        val endTimestamp: Instant,
        val messageList: List<List<PipelineStepObject>>,
        val limit: Int,
        val streamInfo: List<MessageStreamPointer>
    )

    private fun getSearchRequest(
        startTimestamp: Instant, endTimestamp: Instant, resultCount: Int, resumeId: StoredMessageId? = null
    ): SseMessageSearchRequest {
        val parameters = mutableMapOf(
            "stream" to fullStreamName,
            "direction" to listOf(direction),
            "startTimestamp" to listOf(startTimestamp.toString()),
            "endTimestamp" to listOf(endTimestamp.toString()),
            "resultCountLimit" to listOf(resultCount.toString())
        )
        if (resumeId != null) {
            parameters["messageId"] = listOf(resumeId.toString())
        }
        return SseMessageSearchRequest(parameters, FilterPredicate(emptyList()))
    }

    private fun mockContextWithCradleService(): Context {
        val context: Context = mockk()

        every { context.configuration.sendEmptyDelay.value } answers { "1000" }
        every { context.keepAliveTimeout } answers { 200 }

        return context
    }

    private fun getMessageStreams(
        context: Context,
        request: SseMessageSearchRequest,
        scope: CoroutineScope,
        messageList: List<List<PipelineStepObject>>
    ): List<PipelineComponent> {
        return (streamNameObjects zip messageList).map { (streamName, messages) ->
            object : PipelineComponent(context, request, scope, streamName, null, 1) {
                private val messagesStream = messages
                private var last: PipelineStepObject? = null
                private val sequence = sequence {
                    for (message in messagesStream) {
                        last = message
                        yield(message)
                    }
                    while (true) {
                        yield(
                            EmptyPipelineObject(
                                true,
                                last!!.lastProcessedId,
                                Instant.ofEpochMilli(Long.MAX_VALUE)
                            )
                        )
                    }
                }

                init {
                    externalScope.launch {
                        processMessage()
                    }
                }

                override suspend fun processMessage() {
                    for (message in sequence) {
                        sendToChannel(message)
                    }
                }
            }
        }
    }

    private class MutableInstant(timestamp: Instant) {
        var timestamp = timestamp
            private set

        fun getAndAdd(milliseconds: Long = 100): Instant {
            val oldTimestamp = timestamp
            timestamp = timestamp.plusMillis(milliseconds)
            return oldTimestamp
        }
    }

    private fun getMessageGenerator(stream: StreamName, timestamp: MutableInstant): Sequence<PipelineFilteredMessage> {
        return sequence {
            var index = 1L
            val payload = mockk<FilteredMessageWrapper>()
            while (true) {
                val id = StoredMessageId(stream.name, stream.direction, index++)
                yield(PipelineFilteredMessage(false, id, timestamp.getAndAdd(), PipelineStepsInfo(), payload))
            }
        }
    }

    private fun addEmpty(batch: List<PipelineStepObject>): EmptyPipelineObject {
        val last = batch.last()
        return EmptyPipelineObject(false, last.lastProcessedId, last.lastScannedTime)
    }


    private fun getMessages(start: Instant, firstN: Int, secondN: Int): List<List<PipelineStepObject>> {
        val startTimestamp = MutableInstant(start)
        val first = getMessageGenerator(streamNameObjects[0], startTimestamp).take(firstN).toList()
        val second = getMessageGenerator(streamNameObjects[1], startTimestamp).take(secondN).toList()

        val firstStream = mutableListOf<PipelineStepObject>().apply {
            for (msg in first) {
                add(msg)
                add(addEmpty(first))
            }
        }

        val secondStream = mutableListOf<PipelineStepObject>().apply {
            for (msg in second) {
                add(msg)
                add(addEmpty(second))
            }
        }

        return listOf(firstStream, secondStream)
    }


    private fun provideMergeCase(): Iterable<Arguments> {
        val startTimestamp = Instant.parse("2022-04-21T00:00:00Z")
        val endTimestamp = Instant.parse("2022-04-21T01:00:00Z")

        return listOf(
            StreamInfoCase(
                startTimestamp,
                endTimestamp,
                getMessages(startTimestamp, 2, 3),
                limit = 4,
                streamInfo = listOf(
                    MessageStreamPointer(streamNameObjects[0], true, true, StoredMessageId.fromString("${fullStreamName[0]}:-1")),
                    MessageStreamPointer(streamNameObjects[1], true, false, StoredMessageId.fromString("${fullStreamName[1]}:3"))
                )
            ),
            StreamInfoCase(
                startTimestamp,
                endTimestamp,
                getMessages(startTimestamp, 2, 4),
                limit = 4,
                streamInfo = listOf(
                    MessageStreamPointer(streamNameObjects[0], true, true, StoredMessageId.fromString("${fullStreamName[0]}:-1")),
                    MessageStreamPointer(streamNameObjects[1], true, false, StoredMessageId.fromString("${fullStreamName[1]}:3"))
                )
            ),
            StreamInfoCase(
                startTimestamp,
                endTimestamp,
                getMessages(startTimestamp, 2, 4),
                limit = 2,
                streamInfo = listOf(
                    MessageStreamPointer(streamNameObjects[0], true, true, StoredMessageId.fromString("${fullStreamName[0]}:-1")),
                    MessageStreamPointer(streamNameObjects[1], true, false, StoredMessageId.fromString("${fullStreamName[1]}:1"))
                )
            )
        ).map { Arguments { listOf(it).toTypedArray() } }
    }


    @ParameterizedTest
    @MethodSource("provideMergeCase")
    fun `testBorders`(testCase: StreamInfoCase) {

        val context = mockContextWithCradleService()
        val resultMessages = mutableListOf<PipelineFilteredMessage>()
        val request = getSearchRequest(testCase.startTimestamp, testCase.endTimestamp, testCase.limit)

        runBlocking {

            val messageStreams = getMessageStreams(context, request, this, testCase.messageList)

            val streamMerger = StreamMerger(context, request, this, messageStreams, 1, PipelineStatus(context))

            do {
                val message = streamMerger.pollMessage()
                if (message is PipelineFilteredMessage) {
                    resultMessages.add(message)
                }
            } while (message !is StreamEndObject)

            assertArrayEquals(
                testCase.streamInfo.map { it.lastId }.toTypedArray(),
                streamMerger.getStreamsInfo().map { it.lastId }.toTypedArray()
            )
            assertArrayEquals(
                testCase.streamInfo.map { it.hasEnded }.toTypedArray(),
                streamMerger.getStreamsInfo().map { it.hasEnded }.toTypedArray()
            )
            assertArrayEquals(
                testCase.streamInfo.map { it.hasStarted }.toTypedArray(),
                streamMerger.getStreamsInfo().map { it.hasStarted }.toTypedArray()
            )
            coroutineContext.cancelChildren()
        }
    }
}