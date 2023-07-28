/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.rptdataprovider.ProtoContext
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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

    private val BOOK = BookId("")
    private val TIMESTAMP = Instant.now()
    private val TIMESTAMP_EMPTY = Instant.ofEpochMilli(0)

    private val fullStreamName = streamDirection.map { "${baseStreamName}:${it}" }

    private val streamNameObjects = streamDirection.map {
        StreamName(baseStreamName, Direction.valueOf(it.uppercase()), BOOK)
    }

    private val direction = "next"

    data class StreamInfoCase(
        val startTimestamp: Instant,
        val endTimestamp: Instant,
        val messageList: List<List<PipelineStepObject>>,
        val limit: Int,
        val streamInfo: List<StreamInfo>
    )

    private fun getSearchRequest(
        startTimestamp: Instant, endTimestamp: Instant, resultCount: Int, resumeId: StoredMessageId? = null
    ): SseMessageSearchRequest<ProtoRawMessage, Message> {
        val parameters = mutableMapOf(
            "stream" to fullStreamName,
            "direction" to listOf(direction),
            "startTimestamp" to listOf(startTimestamp.toEpochMilli().toString()),
            "endTimestamp" to listOf(endTimestamp.toEpochMilli().toString()),
            "resultCountLimit" to listOf(resultCount.toString()),
            "bookId" to listOf(BOOK.name)
        )
        if (resumeId != null) {
            parameters["messageId"] = listOf(resumeId.toString())
        }
        return SseMessageSearchRequest(parameters, FilterPredicate(emptyList()))
    }


    private fun mockContextWithCradleService(): ProtoContext {
        val context: ProtoContext = mockk()

        every { context.configuration.sendEmptyDelay.value } answers { "1000" }
        every { context.keepAliveTimeout } answers { 200 }

        return context
    }

    private fun getMessageStreams(
        context: ProtoContext,
        request: SseMessageSearchRequest<ProtoRawMessage, Message>,
        scope: CoroutineScope,
        messageList: List<List<PipelineStepObject>>
    ): List<PipelineComponent<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>> {
        return (streamNameObjects zip messageList).map { (streamName, messages) ->
            object : PipelineComponent<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>(context, request, scope, streamName, null, 1) {
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

    private fun getMessageGenerator(stream: StreamName, timestamp: MutableInstant): Sequence<PipelineFilteredMessage<ProtoRawMessage, Message>> {
        return sequence {
            var index = 1L
            val payload = mockk<MessageWithMetadata<ProtoRawMessage, Message>>()
            while (true) {
                val id = StoredMessageId(BOOK, stream.name, stream.direction, TIMESTAMP, index++)
                yield(PipelineFilteredMessage(false, id, timestamp.getAndAdd(), PipelineStepsInfo(), payload))
            }
        }
    }

    private fun addEmpty(last: PipelineStepObject): EmptyPipelineObject {
        return EmptyPipelineObject(false, last.lastProcessedId, last.lastScannedTime)
    }


    private fun getMessages(start: Instant, firstN: Int, secondN: Int): List<List<PipelineStepObject>> {
        val startTimestamp = MutableInstant(start)
        val first = getMessageGenerator(streamNameObjects[0], startTimestamp).take(firstN).toList()
        val second = getMessageGenerator(streamNameObjects[1], startTimestamp).take(secondN).toList()

        val firstStream = mutableListOf<PipelineStepObject>().apply {
            for (msg in first) {
                add(msg)
                add(addEmpty(msg))
            }
        }

        val secondStream = mutableListOf<PipelineStepObject>().apply {
            for (msg in second) {
                add(msg)
                add(addEmpty(msg))
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
                    StreamInfo(streamNameObjects[0], StoredMessageId(BOOK, baseStreamName, Direction.FIRST, TIMESTAMP_EMPTY, -1)),
                    StreamInfo(streamNameObjects[1], StoredMessageId(BOOK, baseStreamName, Direction.SECOND, TIMESTAMP, 3)),
                )
            ),
            StreamInfoCase(
                startTimestamp,
                endTimestamp,
                getMessages(startTimestamp, 2, 4),
                limit = 4,
                streamInfo = listOf(
                    StreamInfo(streamNameObjects[0], StoredMessageId(BOOK, baseStreamName, Direction.FIRST, TIMESTAMP_EMPTY, -1)),
                    StreamInfo(streamNameObjects[1], StoredMessageId(BOOK, baseStreamName, Direction.SECOND, TIMESTAMP, 3)),
                )
            ),
            StreamInfoCase(
                startTimestamp,
                endTimestamp,
                getMessages(startTimestamp, 2, 4),
                limit = 2,
                streamInfo = listOf(
                    StreamInfo(streamNameObjects[0], StoredMessageId(BOOK, baseStreamName, Direction.FIRST, TIMESTAMP_EMPTY, -1)),
                    StreamInfo(streamNameObjects[1], StoredMessageId(BOOK, baseStreamName, Direction.SECOND, TIMESTAMP, 1)),
                )
            )
        ).map { Arguments { listOf(it).toTypedArray() } }
    }


    @ParameterizedTest
    @MethodSource("provideMergeCase")
    fun testBorders(testCase: StreamInfoCase) {

        val context = mockContextWithCradleService()
        val resultMessages = mutableListOf<PipelineFilteredMessage<ProtoRawMessage, Message>>()
        val request = getSearchRequest(testCase.startTimestamp, testCase.endTimestamp, testCase.limit)

        runBlocking {

            val messageStreams = getMessageStreams(context, request, this, testCase.messageList)

            val streamMerger = StreamMerger(context, request, this, messageStreams, 1, PipelineStatus())
            do {
                val message = streamMerger.pollMessage()
                if (message is PipelineFilteredMessage<*, *>) {
                    resultMessages.add(message as PipelineFilteredMessage<ProtoRawMessage, Message>)
                }
            } while (message !is StreamEndObject)

            val sss = streamMerger.getStreamsInfo().map { it.lastElement }.toList()
            println(sss)
            assertArrayEquals(
                testCase.streamInfo.map { it.lastElement }.toTypedArray(),
                streamMerger.getStreamsInfo().map { it.lastElement }.toTypedArray()
            )
            coroutineContext.cancelChildren()
        }
    }
}