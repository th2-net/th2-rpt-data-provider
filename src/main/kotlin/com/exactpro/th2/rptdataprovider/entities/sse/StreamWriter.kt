/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.rptdataprovider.entities.responses.Event
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.eventWrite
import com.exactpro.th2.rptdataprovider.grpc.StreamResponse
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.stub.StreamObserver
import java.io.Writer
import java.util.concurrent.atomic.AtomicLong

interface StreamWriter {

    suspend fun write(event: EventTreeNode, counter: AtomicLong)

    suspend fun write(message: Message, counter: AtomicLong)

    suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong)

    suspend fun write(event: Event, lastEventId: AtomicLong)

    suspend fun closeWriter()
}

class SseWriter(private val writer: Writer, private val jacksonMapper: ObjectMapper) : StreamWriter {

    override suspend fun write(event: EventTreeNode, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, event, counter))
    }

    override suspend fun write(message: Message, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, message, counter))
    }

    override suspend fun write(lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) {
        writer.eventWrite(SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter))
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
            .setEventTreeNode(event.convertToGrpcRptEventTreeNode())
            .build())
        counter.incrementAndGet()
    }

    override suspend fun write(message: Message, counter: AtomicLong) {
        writer.onNext(StreamResponse.newBuilder()
            .setMessage(message.convertToGrpcRptMessage())
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
            .setEvent(event.convertToGrpcRptEvent())
            .build())
        lastEventId.incrementAndGet()
    }

    override suspend fun closeWriter() {
        writer.onCompleted()
    }
}