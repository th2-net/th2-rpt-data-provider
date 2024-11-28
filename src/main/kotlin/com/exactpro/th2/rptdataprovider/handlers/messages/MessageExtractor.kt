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

import com.exactpro.cradle.Order.DIRECT
import com.exactpro.cradle.Order.REVERSE
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.GroupedMessageFilterBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
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
    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()
    private var isStreamEmpty: Boolean = false
    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant? = null

    private val order = when (request.searchDirection) {
        AFTER -> DIRECT
        BEFORE -> REVERSE
    }

    private val sequenceComparator: (Long, Long) -> Boolean = when (order)  {
        DIRECT -> { resumeSequence, messageSequence -> messageSequence >= resumeSequence }
        REVERSE -> { resumeSequence, messageSequence -> messageSequence <= resumeSequence }
    }

    private val timestampComparator = when (order) {
        DIRECT -> Instant::isBefore
        REVERSE -> Instant::isAfter
    }

    private val firstResumeId: Sequence<StreamPointer>.((StreamPointer) -> Instant) -> StreamPointer? =
        when(order) {
            DIRECT -> Sequence<StreamPointer>::minByOrNull
            REVERSE -> Sequence<StreamPointer>::maxByOrNull
        }

    private val emptyStreamTimestamp = when (order) {
        DIRECT -> Instant.MAX
        REVERSE -> Instant.MIN
    }

    init {
        externalScope.launch {
            try {
                processMessage()
            } catch (e: CancellationException) {
                LOGGER.debug { "message extractor for stream $commonStreamName has been stopped" }
            } catch (e: Exception) {
                LOGGER.error(e) { "unexpected exception" }
                throw e
            }
        }
    }

    override suspend fun processMessage() {
        coroutineScope {
            launch {
                while (this@coroutineScope.isActive) {
                    //FIXME: replace delay-based stream updates with synchronous updates from iterator
                    lastTimestamp?.also {
                        sendToChannel(EmptyPipelineObject(isStreamEmpty, lastElement, it).also { msg ->
                            LOGGER.trace { "Extractor has sent EmptyPipelineObject downstream: (lastProcessedId${msg.lastProcessedId} lastScannedTime=${msg.lastScannedTime} streamEmpty=${msg.streamEmpty} hash=${msg.hashCode()})" }
                        })
                    }
                    delay(sendEmptyDelay)
                }
            }

            val resumeFromId = request.resumeFromIdsList.asSequence()
                .filter { it.stream.name == commonStreamName.name }
                .firstResumeId { it.timestamp }

            LOGGER.debug { "acquiring cradle iterator for stream $commonStreamName" }

            resumeFromId?.let { LOGGER.debug { "resume sequence for stream $commonStreamName is set to ${it.sequence}" } }
            request.startTimestamp?.let { LOGGER.debug { "start timestamp for stream $commonStreamName is set to $it" } }

            if (resumeFromId == null || resumeFromId.hasStarted) {
                val (from, to) = when (order) {
                    DIRECT -> request.startTimestamp to request.endTimestamp
                    REVERSE -> request.endTimestamp to request.startTimestamp
                }

                val sessionGroup = context.cradleService.getSessionGroup(
                    commonStreamName.bookId,
                    commonStreamName.name,
                    from,
                    to,
                )

                val cradleMessageIterable = context.cradleService.getGroupedMessages(
                    this,

                    GroupedMessageFilterBuilder()
                        .bookId(commonStreamName.bookId)
                        .groupName(sessionGroup)
                        .order(order)
                        .also { builder ->
                            when (order) {
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

                LOGGER.debug { "cradle iterator has been built for session group: $sessionGroup, alias: $commonStreamName, order: $order" }

                val start = request.startTimestamp
                val end = request.endTimestamp

                var isResumeIdNotFound = resumeFromId != null
                var isStartTimestampNotFound = start != null
                var lastNotFound = true

                for (batch: StoredGroupedMessageBatch in cradleMessageIterable) {
                    if (externalScope.isActive && lastNotFound) {
                        val timeStart = System.currentTimeMillis()

                        pipelineStatus.fetchedStart(commonStreamName.toString())
                        LOGGER.trace { "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName (order: $order) with ${batch.messageCount} messages (${batch.batchSize} bytes) has been extracted" }

                        val orderedMessages = run {
                            val filteredMessages: MutableList<StoredMessage> = ArrayList()
                            batch.messages.filterTo(filteredMessages) { it.sessionAlias == commonStreamName.name }
                            filteredMessages.sortBy { it.timestamp }

                            when (order) {
                                DIRECT -> filteredMessages
                                REVERSE -> filteredMessages.asReversed()
                            }
                        }

                        val resumeIndex = if (isResumeIdNotFound) {
                            requireNotNull(resumeFromId) { "if isResumeIdNotFound is true, resumeFromId is always not null" }

                            val idx = orderedMessages.indexOfFirst { msg -> (msg.direction == resumeFromId.stream.direction && sequenceComparator(resumeFromId.sequence, msg.sequence)) }
                            if (idx == -1) {
                                orderedMessages.size
                            } else {
                                isResumeIdNotFound = false
                                idx
                            }
                        } else 0

                        val startIndex = if (isStartTimestampNotFound) {
                            var idx = resumeIndex
                            while (idx < orderedMessages.size && timestampComparator(orderedMessages[idx].timestamp, start).also { isStartTimestampNotFound = it }) {
                                idx++
                            }
                            idx
                        } else resumeIndex

                        var endIndex = orderedMessages.size
                        if (end != null) {
                            endIndex = startIndex
                            while (endIndex < orderedMessages.size && timestampComparator(orderedMessages[endIndex].timestamp, end).also { lastNotFound = it }) {
                                endIndex++
                            }
                        }

                        val trimmedMessages = when (endIndex - startIndex) {
                            0 -> emptyList()
                            orderedMessages.size -> orderedMessages
                            else -> orderedMessages.subList(startIndex, endIndex)
                        }

                        LOGGER.trace {
                            val messages = if (order == REVERSE) batch.messagesReverse else batch.messages
                            val firstMessage = messages.firstOrNull()
                            val lastMessage = messages.lastOrNull()
                            "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName (order: $order) has been trimmed (targetStartTimestamp=${request.startTimestamp} targetEndTimestamp=${request.endTimestamp} targetId=${resumeFromId?.sequence}) - ${trimmedMessages.size} of ${batch.messageCount} messages left (firstId=${firstMessage?.id?.sequence} firstTimestamp=${firstMessage?.timestamp} lastId=${lastMessage?.id?.sequence} lastTimestamp=${lastMessage?.timestamp})"
                        }

                        pipelineStatus.fetchedEnd(commonStreamName.toString())

                        if (trimmedMessages.isNotEmpty()) {
                            val message = trimmedMessages.last()
                            val rawBatch = PipelineRawBatch(
                                false,
                                message.id,
                                message.timestamp,
                                StoredMessageBatchWrapper(batch.firstTimestamp, trimmedMessages)
                            )
                            rawBatch.info.startExtract = timeStart
                            rawBatch.info.endExtract = System.currentTimeMillis()
                            StreamWriter.setExtract(rawBatch.info)

                            sendToChannel(rawBatch)

                            lastElement = message.id
                            lastTimestamp = message.timestamp

                            LOGGER.trace { "batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName (order: $order) has been sent downstream" }
                        } else {
                            LOGGER.trace { "skipping batch ${batch.firstTimestamp} of group $sessionGroup for stream $commonStreamName (order: $order) - no messages left after trimming" }
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
                        LOGGER.debug { "Exiting $commonStreamName loop. External scope active: '${externalScope.isActive}', LastMessageTrimmed: '${!lastNotFound}'" }
                        break
                    }
                }
            }

            isStreamEmpty = true
            lastTimestamp = emptyStreamTimestamp

            LOGGER.debug { "no more data for stream $commonStreamName (lastId=${lastElement.toString()} lastTimestamp=${lastTimestamp})" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
     }
}