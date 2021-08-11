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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.entities.internal.Message
import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.rptdataprovider.entities.responses.*
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.util.InternalAPI
import io.ktor.util.rootCause
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong


enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS;

    override fun toString(): String {
        return super.toString().toLowerCase()
    }
}

abstract class LastScannedObjectInfo(
    @JsonIgnore var instantTimestamp: Instant? = null,
    var scanCounter: Long = 0
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

    val id: String?
        get() = messageId?.toString() ?: ""

    fun update(message: Message, scanCnt: AtomicLong) {
        messageId = message.id
        instantTimestamp = message.timestamp
        scanCounter = scanCnt.incrementAndGet()
    }

    override fun convertToGrpc(): com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo {
        return com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo.newBuilder()
            .setScanCounter(scanCounter)
            .also { builder ->
                instantTimestamp?.let { builder.setTimestamp(it.toTimestamp()) }
                messageId?.let { builder.setMessageId(it.convertToProto()) }
            }.build()
    }
}


class LastScannedEventInfo(
    @JsonIgnore var eventId: ProviderEventId? = null,
    timestamp: Instant? = null,
    scanCounter: Long = 0
) : LastScannedObjectInfo(timestamp, scanCounter) {

    val id: String?
        get() = eventId?.toString() ?: ""

    fun update(event: BaseEventEntity, scanCnt: AtomicLong) {
        eventId = event.id
        instantTimestamp = event.startTimestamp
        scanCounter = scanCnt.incrementAndGet()
    }


    override fun convertToGrpc(): com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo {
        return com.exactpro.th2.dataprovider.grpc.LastScannedObjectInfo.newBuilder()
            .setScanCounter(scanCounter)
            .also { builder ->
                instantTimestamp?.let { builder.setTimestamp(it.toTimestamp()) }
                id?.let { builder.setEventId(EventID.newBuilder().setId(it)) }
            }
            .build()
    }
}


data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)

/**
 * The data class representing a SSE Event that will be sent to the client.
 */

data class SseEvent(val data: String = "empty data", val event: EventType? = null, val metadata: String? = null) {
    companion object {
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

        suspend fun build(
            jacksonMapper: ObjectMapper,
            streamsInfo: List<StreamInfo>
        ): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(
                    mapOf(
                        "messageIds" to streamsInfo.associate { it.stream to it.lastElement?.toString() }
                    )
                ),
                event = EventType.MESSAGE_IDS
            )
        }

        suspend fun build(
            jacksonMapper: ObjectMapper,
            lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>
        ): SseEvent {
            return SseEvent(
                jacksonMapper.asStringSuspend(
                    mapOf(
                        "messageIds" to lastIdInStream.entries.associate { it.key to it.value?.toString() }
                    )
                ),
                event = EventType.MESSAGE_IDS
            )
        }
    }
}


