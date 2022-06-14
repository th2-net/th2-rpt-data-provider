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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineKeepAlive
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.responses.*
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatusSnapshot
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.util.*
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong


enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS, PIPELINE_STATUS;

    override fun toString(): String {
        return super.toString().toLowerCase()
    }
}

abstract class LastScannedObjectInfo(
    @JsonIgnore protected open var instantTimestamp: Instant? = null,
    open var scanCounter: Long = 0
) {

    val timestamp: Long
        get() = instantTimestamp?.toEpochMilli() ?: 0

    fun update(lastTimestamp: Instant) {
        instantTimestamp = lastTimestamp
    }

    abstract fun convertToGrpc(): com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo

}

class LastScannedMessageInfo(
    @JsonIgnore var messageId: StoredMessageId? = null,
    timestamp: Instant? = null,
    scanCounter: Long = 0
) : LastScannedObjectInfo(timestamp, scanCounter) {

    val id: String
        get() = messageId?.toString() ?: ""

    constructor(pipelineStepObject: PipelineKeepAlive) : this(
        messageId = pipelineStepObject.lastProcessedId,
        timestamp = pipelineStepObject.lastScannedTime,
        scanCounter = pipelineStepObject.scannedObjectsCount
    )

    override fun convertToGrpc(): com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo {
        return com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo.newBuilder()
            .setId(id)
            .setTimestampMillis(timestamp)
            .setScanCounter(scanCounter)
            .build()
    }
}


class LastScannedEventInfo(
    @JsonIgnore var eventId: ProviderEventId? = null,
    timestamp: Instant? = null,
    scanCounter: Long = 0
) : LastScannedObjectInfo(timestamp, scanCounter) {

    val id: String
        get() = eventId?.toString() ?: ""


    fun update(event: BaseEventEntity, scanCnt: AtomicLong) {
        eventId = event.id
        instantTimestamp = event.startTimestamp
        scanCounter = scanCnt.incrementAndGet()
    }

    override fun convertToGrpc(): com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo {
        return com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo.newBuilder()
            .setId(id)
            .setTimestampMillis(timestamp)
            .setScanCounter(scanCounter)
            .build()
    }
}


data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)

/**
 * The data class representing a SSE Event that will be sent to the client.
 */

data class SseEvent(val data: String = "empty data", val event: EventType? = null, val metadata: String? = null) {
    companion object {
        suspend fun build(jacksonMapper: ObjectMapper, status: PipelineStatusSnapshot, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(status),
                EventType.PIPELINE_STATUS,
                counter.incrementAndGet().toString()
            )
        }

        suspend fun build(jacksonMapper: ObjectMapper, event: EventTreeNode, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(event),
                EventType.EVENT,
                counter.incrementAndGet().toString()
            )
        }

        suspend fun build(jacksonMapper: ObjectMapper, event: Event, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(event),
                EventType.EVENT,
                counter.incrementAndGet().toString()
            )
        }

        suspend fun build(jacksonMapper: ObjectMapper, message: HttpMessage, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(message),
                EventType.MESSAGE,
                counter.incrementAndGet().toString()
            )
        }

        suspend fun build(
            jacksonMapper: ObjectMapper,
            lastScannedObjectInfo: LastScannedObjectInfo,
            counter: AtomicLong
        ): SseEvent {
            return SseEvent(
                data = jacksonMapper.asStringSuspend(lastScannedObjectInfo),
                event = EventType.KEEP_ALIVE,
                metadata = counter.incrementAndGet().toString()
            )
        }

        @InternalAPI
        suspend fun build(jacksonMapper: ObjectMapper, e: Exception): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(ExceptionInfo(e.javaClass.name, e.rootCause?.message ?: e.toString())),
                event = EventType.ERROR
            )
        }

        suspend fun build(jacksonMapper: ObjectMapper, streamsInfo: List<StreamInfo>): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(
                    mapOf(
                        "messageIds" to streamsInfo.associate { it.stream.toString() to it.lastElement }
                    )
                ),
                event = EventType.MESSAGE_IDS
            )
        }
    }
}


