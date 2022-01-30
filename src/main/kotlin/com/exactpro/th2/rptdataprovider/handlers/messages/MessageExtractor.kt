/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.time.Instant


class MessageExtractor(
    context: Context,
    val request: SseMessageSearchRequest,
    stream: StreamName,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int,
    private val pipelineStatus: PipelineStatus
) : PipelineComponent(
    context, request, externalScope, stream, messageFlowCapacity = messageFlowCapacity
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()

    private var isStreamEmpty: Boolean = false

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant? = null


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


    override suspend fun processMessage() {
        coroutineScope {
            launch {
                while (this@coroutineScope.isActive) {

                    //FIXME: replace delay-based stream updates with synchronous updates from iterator
                    lastTimestamp?.also {
                        sendToChannel(
                            EmptyPipelineObject(isStreamEmpty, lastElement, it)
                        )
                    }
                    delay(sendEmptyDelay)
                }
            }

            streamName!!

            val resumeFromId = request.resumeFromIdsList.firstOrNull {
                it.streamName.equals(streamName.name) && it.direction.equals(streamName.direction)
            }

            val order = if (request.searchDirection == TimeRelation.AFTER) {
                Order.DIRECT
            } else {
                Order.REVERSE
            }

            logger.debug { "acquiring cradle iterator for stream $streamName" }

            resumeFromId?.let { logger.debug { "resume sequence for stream $streamName is set to ${it.index}" } }
            request.startTimestamp?.let { logger.debug { "start timestamp for stream $streamName is set to $it" } }

            val cradleMessageIterable = context.cradleService.getMessagesBatchesSuspend(
                StoredMessageFilterBuilder().streamName().isEqualTo(streamName.name).direction()
                    .isEqualTo(streamName.direction).order(order)

                    // timestamps will be ignored if resumeFromId is present
                    .also { builder ->
                        if (resumeFromId != null) {
                            builder.index().let {
                                if (order == Order.DIRECT) {
                                    it.isGreaterThan(resumeFromId.index)
                                } else {
                                    it.isLessThan(resumeFromId.index)
                                }
                            }
                        } else {
                            request.startTimestamp?.let { builder.timestampFrom().isGreaterThanOrEqualTo(it) }
                            request.endTimestamp?.let { builder.timestampTo().isLessThanOrEqualTo(it) }
                        }
                    }.build()
            )

            logger.debug { "cradle iterator has been built for stream $streamName" }


            for (batch in cradleMessageIterable) {
                if (externalScope.isActive) {
                    logger.trace { "batch ${batch.id.index} of stream $streamName with ${batch.messageCount} messages (${batch.batchSize} bytes) has been extracted" }

                    val trimmedMessages = let {
                        if (order == Order.DIRECT) {
                            batch.messages
                        } else {
                            batch.messagesReverse
                        }
                    }
                        // trim messages if resumeFromId is present
                        .dropWhile {
                            resumeFromId?.index?.let { start ->
                                if (order == Order.DIRECT) {
                                    it.index <= start
                                } else {
                                    it.index >= start
                                }
                            } ?: false
                        }

                        //trim messages that do not strictly match time filter
                        .dropWhile {
                            request.startTimestamp?.let { startTimestamp ->
                                if (order == Order.DIRECT) {
                                    it.timestamp.isBefore(startTimestamp)
                                } else {
                                    it.timestamp.isAfter(startTimestamp)
                                }
                            } ?: false
                        }

                    val firstMessage = if (order == Order.DIRECT) batch.messages.first() else batch.messages.last()
                    val lastMessage = if (order == Order.DIRECT) batch.messages.last() else batch.messages.first()

                    logger.trace {
                        "batch ${batch.id.index} of stream $streamName has been trimmed - ${trimmedMessages.size} of ${batch.messages.size} messages left (firstId=${firstMessage.id.index} firstTimestamp=${firstMessage.timestamp} lastId=${lastMessage.id.index} lastTimestamp=${lastMessage.timestamp})"
                    }

                    try {
                        trimmedMessages.last().let {
                            sendToChannel(
                                PipelineRawBatch(
                                    false, it.id, it.timestamp, MessageBatchWrapper(batch, trimmedMessages)
                                )
                            )

                            lastElement = it.id
                            lastTimestamp = it.timestamp

                            logger.trace { "batch ${batch.id.index} of stream $streamName has been sent downstream" }

                        }
                    } catch (e: NoSuchElementException) {
                        logger.debug { "skipping batch ${batch.id.index} of stream $streamName - no messages left after trimming" }
                    }

                    pipelineStatus.countFetchedBytes(streamName.toString(), batch.batchSize)
                    pipelineStatus.countFetchedBatches(streamName.toString())
                    pipelineStatus.countFetchedMessages(streamName.toString(), trimmedMessages.size.toLong())
                }
            }

            isStreamEmpty = true
            lastTimestamp = if (order == Order.DIRECT) {
                Instant.MAX
            } else {
                Instant.MIN
            }

            logger.debug { "no more data for stream $streamName (lastId=${lastElement.toString()} lastTimestamp=${lastTimestamp})" }
        }
    }
}
