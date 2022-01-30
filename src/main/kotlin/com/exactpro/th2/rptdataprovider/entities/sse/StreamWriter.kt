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
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.eventWrite
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatusSnapshot
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import java.io.Writer
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue


interface StreamWriter {

    suspend fun write(event: EventTreeNode, counter: AtomicLong)

    suspend fun write(message: MessageWithMetadata, counter: AtomicLong)

    suspend fun write(event: Event, lastEventId: AtomicLong)

    suspend fun write(streamInfo: List<StreamInfo>)

    suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong)

    suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong)

    suspend fun closeWriter()
}

class HttpWriter(private val writer: Writer, private val jacksonMapper: ObjectMapper) : StreamWriter {
    val logger = KotlinLogging.logger { }

    override suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, status, counter))
    }

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, event, counter))
    }

    override suspend fun write(message: MessageWithMetadata, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, MessageMapper.convertToHttpMessage(message), counter))
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter))
    }

    override suspend fun write(streamInfo: List<StreamInfo>) {
        writer.eventWrite(SseEvent.build(jacksonMapper, streamInfo))
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, event, lastEventId))
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
) :
    StreamWriter {

    val logger = KotlinLogging.logger { }

    // yes, channel of deferred responses is required, since the order is critical
    val responses = Channel<Deferred<StreamResponse>>(responseBufferCapacity)

    init {
        scope.launch {
            while (this.isActive) {

                val response = measureTimedValue {
                    responses.receive().await()
                }

                val sendDuration = measureTimeMillis {
                    writer.onNext(response.value)
                }

                logger.trace { "awaited response for ${response.duration.inMilliseconds.roundToInt()}ms, sent in ${sendDuration}ms" }
            }
        }
    }

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        val result = CompletableDeferred<StreamResponse>()
        responses.send(result)

        scope.launch {
            result.complete(
                StreamResponse.newBuilder()
                    .setEventMetadata(event.convertToGrpcEventMetadata())
                    .build()
            )

            counter.incrementAndGet()
        }
    }


    override suspend fun write(message: MessageWithMetadata, counter: AtomicLong) {
        val result = CompletableDeferred<StreamResponse>()
        responses.send(result)

        scope.launch {
            result.complete(
                StreamResponse.newBuilder()
                    .setMessage(MessageMapper.convertToGrpcMessageData(message))
                    .build()
            )

            counter.incrementAndGet()
        }
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        val result = CompletableDeferred<StreamResponse>()
        responses.send(result)

        scope.launch {
            result.complete(
                StreamResponse.newBuilder()
                    .setLastScannedObject(lastScannedObjectInfo.convertToGrpc())
                    .build()
            )

            counter.incrementAndGet()
        }
    }

    override suspend fun write(status: PipelineStatusSnapshot, counter: AtomicLong) {
        logger.info {
            jacksonMapper.writeValueAsString(status)
        }
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        val result = CompletableDeferred<StreamResponse>()
        responses.send(result)

        scope.launch {
            result.complete(
                StreamResponse.newBuilder()
                    .setEvent(event.convertToGrpcEventData())
                    .build()
            )

            lastEventId.incrementAndGet()
        }
    }

    override suspend fun write(streamInfo: List<StreamInfo>) {
        val result = CompletableDeferred<StreamResponse>()
        responses.send(result)

        scope.launch {
            result.complete(
                StreamResponse.newBuilder().setStreamInfo(
                    StreamsInfo.newBuilder().addAllStreams(
                        streamInfo.map { it.convertToProto() }
                    ).build()
                ).build()
            )
        }
    }

    override suspend fun closeWriter() {
        writer.onCompleted()
        logger.debug { "grpc writer has been closed" }
    }
}
