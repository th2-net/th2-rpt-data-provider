/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.sse

import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineFilteredMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepsInfo
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.HttpMessage
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.stub.StreamObserver
import io.prometheus.client.Histogram
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.Writer
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue


interface StreamWriter<RM, PM> {

    companion object {
        val metric: Histogram = Histogram.build(
            "th2_message_pipeline_seconds", "Time of message search pipeline steps"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .labelNames("step_name")
            .register()


        fun setExtract(info: PipelineStepsInfo) {
            metric.labels("extract").observe(info.extractTime().toDouble() / 1000)
        }

        fun setConvert(info: PipelineStepsInfo) {
            metric.labels("convert").observe(info.convertTime().toDouble() / 1000)
        }

        fun setDecodeCodec(info: PipelineStepsInfo) {
            metric.labels("decode_codec").observe(info.decodeCodecResponse().toDouble() / 1000)
        }

        fun setDecodeAll(info: PipelineStepsInfo) {
            metric.labels("decode_all").observe(info.decodeTimeAll().toDouble() / 1000)
        }

        fun setSendToCodecTime(sendingTime: Long) {
            metric.labels("send_to_codec").observe(sendingTime.toDouble() / 1000)
        }

        fun setFilter(info: PipelineStepsInfo) {
            metric.labels("filter").observe(info.filterTime().toDouble() / 1000)
        }

        fun setBuildMessage(info: PipelineStepsInfo) {
            metric.labels("build_message").observe(info.buildMessage.toDouble() / 1000)
        }

        fun setSerializing(info: PipelineStepsInfo) {
            metric.labels("serializing").observe(info.serializingTime.toDouble() / 1000)
        }

        fun setMerging(time: Long) {
            metric.labels("merging").observe(time.toDouble() / 1000)
        }

        fun setSendingTime(sendingTime: Long) {
            metric.labels("sending").observe(sendingTime.toDouble() / 1000)
        }

    }

    suspend fun write(event: EventTreeNode, counter: AtomicLong)

    suspend fun write(message: PipelineFilteredMessage<RM, PM>, counter: AtomicLong)

    suspend fun write(event: Event, lastEventId: AtomicLong)

    suspend fun write(streamInfo: List<StreamInfo>)

    suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong)

    suspend fun closeWriter()
}

class HttpWriter<RM, PM>(
    private val writer: Writer,
    private val jacksonMapper: ObjectMapper,
    private val convertFunc: (MessageWithMetadata<RM, PM>) -> HttpMessage,
) : StreamWriter<RM, PM> {
    val logger = KotlinLogging.logger { }

    fun eventWrite(event: SseEvent) {
        val builder = StringBuilder()
        if (event.event != null) {
            builder.append("event: ").append(event.event).append("\n")
        }

        for (dataLine in event.data.lines()) {
            builder.append("data: ").append(dataLine).append("\n")
        }

        if (event.metadata != null) {
            builder.append("id: ").append(event.metadata).append("\n")
        }

        writer.write(builder.append("\n").toString())
        writer.flush()
    }


    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        eventWrite(SseEvent.build(jacksonMapper, event, counter))
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun write(message: PipelineFilteredMessage<RM, PM>, counter: AtomicLong) {
        val convertedMessage: TimedValue<HttpMessage> = measureTimedValue { convertFunc(message.payload) }
        message.info.serializingTime = convertedMessage.duration.inWholeMilliseconds
        StreamWriter.setSerializing(message.info)

        val sendingTime = measureTimeMillis {
            eventWrite(SseEvent.build(jacksonMapper, convertedMessage.value, counter))
        }

        StreamWriter.setSendingTime(sendingTime)

    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        eventWrite(SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter))
    }

    override suspend fun write(streamInfo: List<StreamInfo>) {
        eventWrite(SseEvent.build(jacksonMapper, streamInfo))
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        eventWrite(SseEvent.build(jacksonMapper, event, lastEventId))
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override suspend fun closeWriter() {
        writer.close()
        logger.debug { "http sse writer has been closed" }
    }
}

@OptIn(ExperimentalTime::class)
class GrpcWriter<RM, PM>(
    responseBufferCapacity: Int,
    private val writer: StreamObserver<StreamResponse>,
    private val writerPool: CoroutineScope,
    private val converFunc: (MessageWithMetadata<RM, PM>) -> List<MessageData>
) : StreamWriter<RM, PM> {
    //FIXME: List<StreamResponse> is a temporary solution to send messages group without merging the content

    // yes, channel of deferred responses is required, since the order is critical
    private val responses = Channel<Deferred<List<StreamResponse>>>(responseBufferCapacity)

    companion object {
//        private const val poolSize = 5

        private val logger = KotlinLogging.logger { }
//
//        val handler = CoroutineExceptionHandler { _, exception ->
//            logger.error(exception) { "GRPC send error" }
//        }
//
//        private val writerPool = CoroutineScope(
//            Executors.newFixedThreadPool(poolSize).asCoroutineDispatcher() + handler
//        )
    }


    init {
        writerPool.launch {
            for (response in responses) {
                val awaited = measureTimedValue {
                    response.await()
                }

                val sendDuration = measureTimeMillis {
                    awaited.value.forEach { writer.onNext(it) }
                }

                StreamWriter.setSendingTime(sendDuration)

                logger.trace { "awaited response for ${awaited.duration.toDouble(DurationUnit.MILLISECONDS).roundToInt()}ms, sent in ${sendDuration}ms" }
            }
        }
    }

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        writerPool.launch {
            result.complete(
                listOf(
                    StreamResponse.newBuilder()
                        .setEventMetadata(event.convertToGrpcEventMetadata())
                        .build()
                )
            )

            counter.incrementAndGet()
        }
    }


    override suspend fun write(message: PipelineFilteredMessage<RM, PM>, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        writerPool.launch {
            result.complete(
                let {
                    val convertedMessage = measureTimedValue {
                        converFunc(message.payload)
                            .map { StreamResponse.newBuilder().setMessage(it).build() }
                    }
                    message.info.serializingTime = convertedMessage.duration.inWholeMilliseconds
                    StreamWriter.setSerializing(message.info)

                    convertedMessage.value
                }
            )
            counter.incrementAndGet()
        }
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        writerPool.launch {
            result.complete(
                listOf(
                    StreamResponse.newBuilder()
                        .setLastScannedObject(lastScannedObjectInfo.convertToGrpc())
                        .build()
                )
            )

            counter.incrementAndGet()
        }
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        writerPool.launch {
            result.complete(
                listOf(
                    StreamResponse.newBuilder()
                        .setEvent(event.convertToGrpcEventData())
                        .build()
                )
            )

            lastEventId.incrementAndGet()
        }
    }

    override suspend fun write(streamInfo: List<StreamInfo>) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        writerPool.launch {
            result.complete(
                listOf(
                    StreamResponse.newBuilder().setStreamInfo(
                        StreamsInfo.newBuilder().addAllStreams(
                            streamInfo.map { it.convertToProto() }
                        ).build()
                    ).build()
                )
            )
        }
    }

    override suspend fun closeWriter() {
        responses.close()
        logger.debug { "grpc writer has been closed" }
    }
}
