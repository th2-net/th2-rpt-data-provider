/*
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

import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.GroupedMessageFilterBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
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


class MessageExtractor<B, G, RM, PM>(
    context: Context<B, G, RM, PM>,
    val request: SseMessageSearchRequest<RM, PM>,
    override val commonStreamName: CommonStreamName,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent<B, G, RM, PM>(
    context, request, externalScope, commonStreamName, messageFlowCapacity = messageFlowCapacity
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()

    private var isStreamEmpty: Boolean = false

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant? = null

    private val order = if (request.searchDirection == AFTER) {
        Order.DIRECT
    } else {
        Order.REVERSE
    }

    init {
        externalScope.launch {
            try {
                processMessage()
            } catch (e: CancellationException) {
                logger.debug { "message extractor for stream $commonStreamName has been stopped" }
            } catch (e: Exception) {
                logger.error(e) { "unexpected exception" }
                throw e
            }
        }
    }

    private val StoredGroupedMessageBatch.orderedMessages: Collection<StoredMessage>
        get() = if (order == Order.DIRECT) messages else messagesReverse

    private val StoredMessage.isNotCorrectAlias: Boolean
        get() = commonStreamName?.let { sessionAlias != it.name } ?: false

    private fun StoredMessage.isNotResume(point: StreamPointer?): Boolean =
        point?.let {
            point.sequence == sequence &&
                    point.timestamp == timestamp
                    point.stream.name == sessionAlias &&
                    point.stream.direction == direction


        } ?: false

    private val StoredMessage.isEarlierStart
        get() = request.startTimestamp?.let {
            if (order == Order.DIRECT) timestamp.isBefore(it) else timestamp.isAfter(it)
        } ?: false

    private val StoredMessage.isLaterEnd
        get() = request.endTimestamp?.let {
            if (order == Order.DIRECT) timestamp.isAfterOrEqual(it) else timestamp.isBeforeOrEqual(it)
        } ?: false

    private fun getMessagesFromBatch(batch: StoredMessageBatch): Collection<StoredMessage> {
        return if (order == Order.DIRECT) {
            batch.messages
        } else {
            batch.messagesReverse
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
                    if (order == Order.DIRECT) {
                        it.sequence < startSeq
                    } else {
                        it.sequence > startSeq
                    }
                }
            } else {
                this // nothing to filter by sequence
            }
        }.dropWhile { //trim messages that do not strictly match time filter
            request.startTimestamp?.let { startTimestamp ->
                if (order == Order.DIRECT) {
                    it.timestamp.isBefore(startTimestamp)
                } else {
                    it.timestamp.isAfter(startTimestamp)
                }
            } ?: false
        }
    }

    private fun trimMessagesListTail(message: StoredMessage): Boolean {
        return request.endTimestamp?.let { endTimestamp ->
            if (order == Order.DIRECT) {
                message.timestamp.isAfterOrEqual(endTimestamp)
            } else {
                message.timestamp.isBeforeOrEqual(endTimestamp)
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

            commonStreamName!!

            val resumeFromId = request.resumeFromIdsList.asSequence()
                .filter { it.stream.name == commonStreamName.name }
                .run {
                    when(request.searchDirection) {
                        BEFORE -> maxByOrNull { it.timestamp }
                        AFTER -> minByOrNull { it.timestamp }
                    }
                }

            logger.debug { "acquiring cradle iterator for stream $commonStreamName" }

            resumeFromId?.let { logger.debug { "resume sequence for stream $commonStreamName is set to ${it.sequence}" } }
            request.startTimestamp?.let { logger.debug { "start timestamp for stream $commonStreamName is set to $it" } }

            if (resumeFromId == null || resumeFromId.hasStarted) {
                val sessionGroup = context.cradleService.getSessionGroup(
                    commonStreamName.bookId,
                    commonStreamName.name,
                    request.startTimestamp,
                    request.endTimestamp,
                )
                val cradleMessageIterable = context.cradleService.getGroupedMessages(
                    this,
                    GroupedMessageFilterBuilder().apply {
                        groupName(
                            sessionGroup,
                        )
                        order(order)

                        if (order == Order.DIRECT) {
                            request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                            request.endTimestamp?.let { timestampTo().isLessThan(it) }
                        } else {
                            request.startTimestamp?.let { timestampTo().isLessThanOrEqualTo(it) }
                            request.endTimestamp?.let { timestampFrom().isGreaterThan(it) }
                        }
                    }.build()
                )
//                getMessagesBatchesSuspend(
//                    MessageFilterBuilder()
//                        .sessionAlias(streamName.name)
//                        .direction(streamName.direction)
//                        .order(order)
//                        .bookId(streamName.bookId)

                        // timestamps will be ignored if resumeFromId is present
//                        .also { builder ->
//                            if (resumeFromId != null) {
//                                builder.sequence().let {
//                                    if (order == Order.DIRECT) {
//                                        it.isGreaterThanOrEqualTo(resumeFromId.sequence)
//                                    } else {
//                                        it.isLessThanOrEqualTo(resumeFromId.sequence)
//                                    }
//                                }
//                            }
//                            // always need to make sure that we send messages within the specified timestamp (in case the resume ID points to the past)
//                            if (order == Order.DIRECT) {
//                                request.startTimestamp?.let { builder.timestampFrom().isGreaterThanOrEqualTo(it) }
//                                request.endTimestamp?.let { builder.timestampTo().isLessThan(it) }
//                            } else {
//                                request.startTimestamp?.let { builder.timestampTo().isLessThanOrEqualTo(it) }
//                                request.endTimestamp?.let { builder.timestampFrom().isGreaterThan(it) }
//                            }
//                        }.build()
//                )

                logger.debug { "cradle iterator has been built for session group: $sessionGroup, alias: $commonStreamName" }

                var firstFound = false
                var lastFound = false
                var isLastMessageTrimmed = false

                for (batch: StoredGroupedMessageBatch in cradleMessageIterable) {
                    if (externalScope.isActive && !lastFound) {

                        val timeStart = System.currentTimeMillis()

                        pipelineStatus.fetchedStart(commonStreamName.toString())
                        logger.trace { "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName with ${batch.messageCount} messages (${batch.batchSize} bytes) has been extracted" }

                        val trimmedMessages = ArrayList<StoredMessage>(batch.messageCount)
                        batch.orderedMessages.forEach { msg ->
                            if (msg.isNotCorrectAlias) { return@forEach }

                            if (!firstFound) {
                                if (msg.isEarlierStart) { return@forEach }
                                if (msg.isNotResume(resumeFromId)) { return@forEach }
                                firstFound = true
                            }

                            if(!lastFound) {
                                if (msg.isLaterEnd) {
                                    lastFound = true
                                    return@forEach
                                }
                            }

                            trimmedMessages.add(msg)
                        }

                        val firstMessage = if (order == Order.DIRECT) batch.messages.first() else batch.messages.last()
                        val lastMessage = if (order == Order.DIRECT) batch.messages.last() else batch.messages.first()

                        logger.trace {
                            "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName has been trimmed (targetStartTimestamp=${request.startTimestamp} targetEndTimestamp=${request.endTimestamp} targetId=${resumeFromId?.sequence}) - ${trimmedMessages.size} of ${batch.messageCount} messages left (firstId=${firstMessage.id.sequence} firstTimestamp=${firstMessage.timestamp} lastId=${lastMessage.id.sequence} lastTimestamp=${lastMessage.timestamp})"
                        }

                        pipelineStatus.fetchedEnd(commonStreamName.toString())

                        try {
                            trimmedMessages.last().let { message ->
                                sendToChannel(PipelineRawBatch(
                                    false,
                                    message.id,
                                    message.timestamp,
                                    StoredMessageBatchWrapper(batch.firstTimestamp, trimmedMessages)
                                ).also {
                                    it.info.startExtract = timeStart
                                    it.info.endExtract = System.currentTimeMillis()
                                    StreamWriter.setExtract(it.info)
                                })
                                lastElement = message.id
                                lastTimestamp = message.timestamp

                                logger.trace { "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName has been sent downstream" }

                            }
                        } catch (e: NoSuchElementException) {
                            logger.trace { "skipping batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName - no messages left after trimming" }
                            pipelineStatus.countSkippedBatches(commonStreamName.toString())
                        }
                        pipelineStatus.fetchedSendDownstream(commonStreamName.toString())

                        pipelineStatus.countFetchedBytes(commonStreamName.toString(), batch.batchSize.toLong())
                        pipelineStatus.countFetchedBatches(commonStreamName.toString())
                        pipelineStatus.countFetchedMessages(commonStreamName.toString(), trimmedMessages.size.toLong())
                        pipelineStatus.countSkippedMessages(
                            commonStreamName.toString(), batch.messageCount - trimmedMessages.size.toLong()
                        )
                    } else {
                        logger.debug { "Exiting $commonStreamName loop. External scope active: '${externalScope.isActive}', LastMessageTrimmed: '$isLastMessageTrimmed'" }
                        break
                    }
                }
            }

            isStreamEmpty = true
            lastTimestamp = if (order == Order.DIRECT) {
                Instant.ofEpochMilli(Long.MAX_VALUE)
            } else {
                Instant.ofEpochMilli(Long.MIN_VALUE)
            }

            logger.debug { "no more data for stream $commonStreamName (lastId=${lastElement.toString()} lastTimestamp=${lastTimestamp})" }
        }
    }
}
