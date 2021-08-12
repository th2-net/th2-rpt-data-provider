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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StreamsInfo
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.mappers.MessageMapper
import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.eventWrite
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.stub.StreamObserver
import java.io.Writer
import java.util.concurrent.atomic.AtomicLong


interface StreamWriter {

    suspend fun write(event: EventTreeNode, counter: AtomicLong)

    suspend fun write(message: MessageWithMetadata, counter: AtomicLong)

    suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong)

    suspend fun write(streamInfo: MutableList<StreamInfo>)

    suspend fun write(lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>)

    suspend fun write(event: Event, lastEventId: AtomicLong)

    suspend fun closeWriter()
}

class SseWriter(private val writer: Writer, private val jacksonMapper: ObjectMapper) : StreamWriter {

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, event, counter))
    }

    override suspend fun write(message: MessageWithMetadata, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, MessageMapper.convertToHttpMessage(message), counter))
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter))
    }

    override suspend fun write(streamInfo: MutableList<StreamInfo>) {
        writer.eventWrite(SseEvent.build(jacksonMapper, streamInfo))
    }

    override suspend fun write(lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>) {
        writer.eventWrite(SseEvent.build(jacksonMapper, lastIdInStream))
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, event, lastEventId))
    }

    override suspend fun closeWriter() {
        writer.close()
    }
}

class GrpcWriter(private val writer: StreamObserver<StreamResponse>) : StreamWriter {
    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        writer.onNext(StreamResponse.newBuilder()
            .setEventMetadata(event.convertToGrpcEventMetadata())
            .build())
        counter.incrementAndGet()
    }

    override suspend fun write(message: MessageWithMetadata, counter: AtomicLong) {
        writer.onNext(StreamResponse.newBuilder()
            .setMessage(MessageMapper.convertToGrpcMessageData(message))
            .build())
        counter.incrementAndGet()
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        writer.onNext(StreamResponse.newBuilder()
            .setLastScannedObject(lastScannedObjectInfo.convertToGrpc())
            .build())
        counter.incrementAndGet()
    }

    override suspend fun write(event: Event, lastEventId: AtomicLong) {
        writer.onNext(StreamResponse.newBuilder()
            .setEvent(event.convertToGrpcEventData())
            .build())
        lastEventId.incrementAndGet()
    }

    override suspend fun write(streamInfo: MutableList<StreamInfo>) {
        writer.onNext(
            StreamResponse.newBuilder().setStreamInfo(
                StreamsInfo.newBuilder().addAllStreams(
                    streamInfo.map { it.convertToProto() }
                ).build()
            ).build()
        )
    }

    override suspend fun write(lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>) {
        writer.onNext(
            StreamResponse.newBuilder().setStreamInfo(
                StreamsInfo.newBuilder().addAllStreams(
                    lastIdInStream.entries.map { (stream, lastElement) ->
                        Stream.newBuilder()
                            .setDirection(cradleDirectionToGrpc(stream.second))
                            .setSession(stream.first).also { builder ->
                                lastElement?.let { builder.setLastId(it.convertToProto()) }
                            }.build()
                    }
                ).build()
            ).build()
        )
    }

    override suspend fun closeWriter() {
        writer.onCompleted()
    }
}