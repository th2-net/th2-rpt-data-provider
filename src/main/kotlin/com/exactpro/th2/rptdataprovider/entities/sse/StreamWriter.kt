/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.sse

import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineFilteredMessage
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatusSnapshot
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.stub.StreamObserver
import io.prometheus.client.Histogram
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.io.Writer
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue


interface StreamWriter {

    companion object {
        val metric =  Histogram.build(
            "th2_message_pipeline_seconds", "Time of message search pipeline steps"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .labelNames("step_name")
            .register()

        fun setMetrics(message: PipelineFilteredMessage) {
            with(message.info) {
                metric.labels("extract").observe(extractTime().toDouble())
                metric.labels("convert").observe(convertTime().toDouble())
                metric.labels("decode_codec").observe(decodeCodecResponse().toDouble())
                metric.labels("decode_all").observe(decodeTimeAll().toDouble())
                metric.labels("filter").observe(filterTime().toDouble())
                metric.labels("serializing").observe(serializingTime.toDouble())
            }
        }

        fun setSendingTime(sendingTime: Long) {
            metric.labels("sending").observe(sendingTime.toDouble())
        }

    }

    suspend fun write(event: EventTreeNode, counter: AtomicLong)

    suspend fun write(message: PipelineFilteredMessage, counter: AtomicLong)

    suspend fun write(event: Event, lastEventId: AtomicLong)

    suspend fun write(streamInfo: List<StreamInfo>)

    suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong)

    suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong)

    suspend fun closeWriter()
}

class HttpWriter(private val writer: Writer, private val jacksonMapper: ObjectMapper) : StreamWriter {
    val logger = KotlinLogging.logger { }

    fun eventWrite(event: SseEvent) {
        if (event.event != null) {
            writer.write("event: ${event.event}\n")
        }

        for (dataLine in event.data.lines()) {
            writer.write("data: $dataLine\n")
        }

        if (event.metadata != null) {
            writer.write("id: ${event.metadata}\n")
        }

        writer.write("\n")
        writer.flush()
    }

    override suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong) {
        eventWrite(SseEvent.build(jacksonMapper, status, counter))
        logger.debug {
            jacksonMapper.writeValueAsString(status)
        }
    }

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        eventWrite(SseEvent.build(jacksonMapper, event, counter))
    }

    @OptIn(ExperimentalTime::class)
    override suspend fun write(message: PipelineFilteredMessage, counter: AtomicLong) {
        val convertedMessage = measureTimedValue {
            MessageMapper.convertToHttpMessage(message.payload)
        }
        message.info.serializingTime = convertedMessage.duration.toLongMilliseconds()

        val sendingTime = measureTimeMillis {
            eventWrite(SseEvent.build(jacksonMapper, convertedMessage.value, counter))
        }

        StreamWriter.setMetrics(message)
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
class GrpcWriter(
    responseBufferCapacity: Int,
    private val writer: StreamObserver<StreamResponse>,
    private val jacksonMapper: ObjectMapper,
    private val scope: CoroutineScope
) : StreamWriter {

    private val logger = KotlinLogging.logger { }

    //FIXME: List<StreamResponse> is a temporary solution to send messages group without merging the content

    // yes, channel of deferred responses is required, since the order is critical
    private val responses = Channel<Deferred<List<StreamResponse>>>(responseBufferCapacity)

    init {
        scope.launch {
            for (response in responses) {
                val awaited = measureTimedValue {
                    response.await()
                }

                val sendDuration = measureTimeMillis {
                    awaited.value.forEach { writer.onNext(it) }
                }

                StreamWriter.setSendingTime(sendDuration)

                logger.trace { "awaited response for ${awaited.duration.inMilliseconds.roundToInt()}ms, sent in ${sendDuration}ms" }
            }
            writer.onCompleted()
            logger.debug { "grpc stream onCompleted() has been called" }
        }
    }

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        scope.launch {
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


    override suspend fun write(message: PipelineFilteredMessage, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        scope.launch {
            result.complete(
                let {
                    val convertedMessage = measureTimedValue {
                        MessageMapper.convertToGrpcMessageData(message.payload)
                            .map { StreamResponse.newBuilder().setMessage(it).build() }
                    }
                    message.info.serializingTime = convertedMessage.duration.toLongMilliseconds()
                    StreamWriter.setMetrics(message)

                    convertedMessage.value
                }
            )
            counter.incrementAndGet()
        }
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        scope.launch {
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

    override suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong) {
        logger.debug {
            jacksonMapper.writeValueAsString(status)
        }
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        val result = CompletableDeferred<List<StreamResponse>>()
        responses.send(result)

        scope.launch {
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

        scope.launch {
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
