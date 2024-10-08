﻿/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.Order.DIRECT
import com.exactpro.cradle.Order.REVERSE
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.internal.StreamName
import com.exactpro.th2.rptdataprovider.entities.internal.StreamPointer
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StoredMessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.isAfterOrEqual
import com.exactpro.th2.rptdataprovider.isBeforeOrEqual
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Instant
import com.exactpro.cradle.Order as CradleOrder


class MessageExtractor<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    val request: SseMessageSearchRequest<RM, PM>,
    stream: StreamName,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent<B, G, RM, PM>(
    context, request, externalScope, stream, messageFlowCapacity = messageFlowCapacity
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()

    private var isStreamEmpty: Boolean = false

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant? = null

    private val order: CradleOrder = when(request.searchDirection) {
        BEFORE -> REVERSE
        AFTER -> DIRECT
    }

    init {
        externalScope.launch {
            try {
                processMessage()
            } catch (e: CancellationException) {
                logger.debug { "message extractor for stream $streamName has been stopped" }
            } catch (e: Exception) {
                logger.error(e) { "unexpected exception" }
                throw e
            }
        }
    }

    private fun getMessagesFromBatch(batch: StoredMessageBatch): Collection<StoredMessage> {
        return when(order) {
            DIRECT -> batch.messages
            REVERSE -> batch.messagesReverse
        }
    }

    private fun trimMessagesListHead(
        messages: Collection<StoredMessage>,
        resumeFromId: StreamPointer?
    ): List<StoredMessage> {
        return messages.run {
            // trim messages if resumeFromId is present
            if (resumeFromId?.sequence != null) {
                val startSeq = resumeFromId.sequence
                dropWhile {
                    when(order) {
                        DIRECT -> it.sequence < startSeq
                        REVERSE -> it.sequence > startSeq
                    }
                }
            } else {
                this // nothing to filter by sequence
            }
        }.dropWhile { //trim messages that do not strictly match time filter
            request.startTimestamp?.let { startTimestamp ->
                when(order) {
                    DIRECT -> it.timestamp.isBefore(startTimestamp)
                    REVERSE -> it.timestamp.isAfter(startTimestamp)
                }
            } ?: false
        }
    }

    private fun trimMessagesListTail(message: StoredMessage): Boolean {
        return request.endTimestamp?.let { endTimestamp ->
            when(order) {
                DIRECT -> message.timestamp.isAfterOrEqual(endTimestamp)
                REVERSE -> message.timestamp.isBeforeOrEqual(endTimestamp)
            }
        } ?: false
    }

    override suspend fun processMessage() {
        coroutineScope {
            launch {
                while (this@coroutineScope.isActive) {

                    //FIXME: replace delay-based stream updates with synchronous updates from iterator
                    lastTimestamp?.also {
                        sendToChannel(EmptyPipelineObject(isStreamEmpty, lastElement, it).also { msg ->
                            logger.trace { "Extractor has sent EmptyPipelineObject downstream: (lastProcessedId${msg.lastProcessedId} lastScannedTime=${msg.lastScannedTime} streamEmpty=${msg.streamEmpty} hash=${msg.hashCode()})" }
                        })
                    }
                    delay(sendEmptyDelay)
                }
            }

            streamName!!

            val resumeFromId = request.resumeFromIdsList.asSequence()
                .filter { it.stream.name == streamName.name && it.stream.direction == streamName.direction }
                .run {
                    when(order) {
                        DIRECT -> minByOrNull(StreamPointer::sequence)
                        REVERSE -> maxByOrNull(StreamPointer::sequence)
                    }
                }

            logger.debug { "acquiring cradle iterator for stream $streamName" }

            resumeFromId?.let { logger.debug { "resume sequence for stream $streamName is set to ${it.sequence}" } }
            request.startTimestamp?.let { logger.debug { "start timestamp for stream $streamName is set to $it" } }

            if (resumeFromId == null || resumeFromId.hasStarted) {
                val cradleMessageIterable = context.cradleService.getMessagesBatchesSuspend(
                    MessageFilterBuilder()
                        .sessionAlias(streamName.name)
                        .direction(streamName.direction)
                        .order(order)
                        .bookId(streamName.bookId)

                        // timestamps will be ignored if resumeFromId is present
                        .also { builder ->
                            if (resumeFromId != null) {
                                builder.sequence().let {
                                    when(order) {
                                        DIRECT -> it.isGreaterThanOrEqualTo(resumeFromId.sequence)
                                        REVERSE -> it.isLessThanOrEqualTo(resumeFromId.sequence)
                                    }
                                }
                            }
                            // always need to make sure that we send messages within the specified timestamp (in case the resume ID points to the past)
                            when(order) {
                                DIRECT -> {
                                    request.startTimestamp?.let { builder.timestampFrom().isGreaterThanOrEqualTo(it) }
                                    request.endTimestamp?.let { builder.timestampTo().isLessThan(it) }
                                }
                                REVERSE -> {
                                    request.startTimestamp?.let { builder.timestampTo().isLessThanOrEqualTo(it) }
                                    request.endTimestamp?.let { builder.timestampFrom().isGreaterThan(it) }
                                }
                            }
                        }.build()
                )

                logger.debug { "cradle iterator has been built for stream $streamName" }

                var isLastMessageTrimmed = false

                for (batch in cradleMessageIterable) {
                    if (externalScope.isActive && !isLastMessageTrimmed) {

                        val timeStart = System.currentTimeMillis()

                        pipelineStatus.fetchedStart(streamName.toString())
                        logger.trace { "batch ${batch.id.sequence} of stream $streamName with ${batch.messageCount} messages (${batch.batchSize} bytes) has been extracted" }

                        val trimmedMessages = trimMessagesListHead(getMessagesFromBatch(batch), resumeFromId)
                            .dropLastWhile { message ->
                                trimMessagesListTail(message).also {
                                    isLastMessageTrimmed = isLastMessageTrimmed || it
                                }
                            }

                        val firstMessage = when(order) {
                            DIRECT -> batch.messages.first()
                            REVERSE -> batch.messages.last()
                        }
                        val lastMessage = when(order) {
                            DIRECT -> batch.messages.last()
                            REVERSE -> batch.messages.first()
                        }

                        logger.trace {
                            "batch ${batch.id.sequence} of stream $streamName has been trimmed (targetStartTimestamp=${request.startTimestamp} targetEndTimestamp=${request.endTimestamp} targetId=${resumeFromId?.sequence}) - ${trimmedMessages.size} of ${batch.messages.size} messages left (firstId=${firstMessage.id.sequence} firstTimestamp=${firstMessage.timestamp} lastId=${lastMessage.id.sequence} lastTimestamp=${lastMessage.timestamp})"
                        }

                        pipelineStatus.fetchedEnd(streamName.toString())

                        try {
                            trimmedMessages.last().let { message ->
                                sendToChannel(PipelineRawBatch(
                                    false,
                                    message.id,
                                    message.timestamp,
                                    StoredMessageBatchWrapper(batch.id, trimmedMessages)
                                ).also {
                                    it.info.startExtract = timeStart
                                    it.info.endExtract = System.currentTimeMillis()
                                    StreamWriter.setExtract(it.info)
                                })
                                lastElement = message.id
                                lastTimestamp = message.timestamp

                                logger.trace { "batch ${batch.id.sequence} of stream $streamName has been sent downstream" }

                            }
                        } catch (e: NoSuchElementException) {
                            logger.trace { "skipping batch ${batch.id.sequence} of stream $streamName - no messages left after trimming" }
                            pipelineStatus.countSkippedBatches(streamName.toString())
                        }
                        pipelineStatus.fetchedSendDownstream(streamName.toString())

                        pipelineStatus.countFetchedBytes(streamName.toString(), batch.batchSize.toLong())
                        pipelineStatus.countFetchedBatches(streamName.toString())
                        pipelineStatus.countFetchedMessages(streamName.toString(), trimmedMessages.size.toLong())
                        pipelineStatus.countSkippedMessages(
                            streamName.toString(), batch.messageCount - trimmedMessages.size.toLong()
                        )
                    } else {
                        logger.debug { "Exiting $streamName loop. External scope active: '${externalScope.isActive}', LastMessageTrimmed: '$isLastMessageTrimmed'" }
                        break
                    }
                }
            }

            isStreamEmpty = true
            lastTimestamp = when(order) {
                DIRECT -> Instant.ofEpochMilli(Long.MAX_VALUE)
                REVERSE -> Instant.ofEpochMilli(Long.MIN_VALUE)
            }

            logger.debug { "no more data for stream $streamName (lastId=${lastElement.toString()} lastTimestamp=${lastTimestamp})" }
        }
    }
}
