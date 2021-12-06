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

import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.lang.Integer.min
import java.time.Instant


class MessageContinuousStream(
    private val startMessageId: StoredMessageId?,
    private val startTimestamp: Instant,
    private val stream: StreamName,
    context: Context,
    request: SseMessageSearchRequest,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int
) : PipelineComponent(startMessageId, context, request, externalScope, stream, messageFlowCapacity = messageFlowCapacity) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private var messageLoader: MessageLoader? = null

    private val sseSearchDelay = context.configuration.sseSearchDelay.value.toLong()
    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()
    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = maxMessagesLimit// min(maxMessagesLimit, searchRequest.resultCountLimit ?: 25)

    private var firstPull: Boolean = true
    private var isStreamEmpty: Boolean = false
    private var needLoadMessage: Boolean = true

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant = startTimestamp
    private var firstMessageInRequest: StoredMessage? = null


    init {
        externalScope.launch {
            processMessage()
        }
    }

    private data class StreamState(
        val firstMessageInRequest: StoredMessage?,
        val lastMessageInRequest: StoredMessageId?,
        val lastTimestamp: Instant
    )

    private suspend fun initialize() {
        if (startMessageId != null) {
            context.cradleService.getMessageSuspend(startMessageId)?.let {
                lastElement = it.id
                lastTimestamp = it.timestamp
                firstPull = false
            }
        }
        messageLoader = MessageLoader(context, stream, startTimestamp, searchRequest)
    }

    private fun tryToLoadStreamAfter() {
        if (lastElement == null || isStreamEmpty) {
            lastTimestamp = Instant.now()
        }
    }

    private fun tryToLoadStreamBefore() {
        if (lastElement == null || isStreamEmpty) {
            lastTimestamp = Instant.MIN
            needLoadMessage = false
        }
    }

    private fun tryToReloadStream() {
        if (searchRequest.searchDirection == TimeRelation.AFTER) {
            tryToLoadStreamAfter()
        } else {
            tryToLoadStreamBefore()
        }
    }

    private fun getNewStreamState(filteredIdsList: List<MessageBatchWrapper>): StreamState {
        val lastBatchWrapper = filteredIdsList.lastOrNull { !it.messageBatch.isEmpty }
        val firstBatchWrapper = filteredIdsList.firstOrNull { !it.messageBatch.isEmpty }

        return if (lastBatchWrapper == null) {
            logger.trace { lastElement }
            StreamState(null, lastElement, lastTimestamp)
        } else {
            val (firstMessage, lastMessage) =
                if (searchRequest.searchDirection == TimeRelation.AFTER) {
                    firstBatchWrapper!!.messageBatch.firstMessage to
                            lastBatchWrapper.messageBatch.lastMessage
                } else {
                    firstBatchWrapper!!.messageBatch.lastMessage to
                            lastBatchWrapper.messageBatch.firstMessage
                }
            StreamState(firstMessage, lastMessage.id, lastMessage.timestamp)
        }
    }

    private fun changeStreamState(messages: List<MessageBatchWrapper>) {
        isStreamEmpty = messages.isEmpty()

        getNewStreamState(messages).let {
            logger.trace { it.lastMessageInRequest }
            firstMessageInRequest = it.firstMessageInRequest
            lastElement = it.lastMessageInRequest
            lastTimestamp = it.lastTimestamp
        }
    }

    private suspend fun loadMoreMessage(): List<MessageBatchWrapper> {
        if (lastElement == null || messageLoader == null) {
            return emptyList()
        }

        val messages = if (lastElement != null) {
            messageLoader!!.pullMoreMessageById(lastElement!!, firstPull, perStreamLimit)
        } else {
            messageLoader!!.pullMoreMessageById(lastElement!!, firstPull, perStreamLimit)
        }

        return messages.also {
            if (it.isNotEmpty()) {
                firstPull = false
                perStreamLimit = min(perStreamLimit * 2, maxMessagesLimit)
            }
        }
    }

    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            sendToChannel(
                EmptyPipelineObject(
                    isStreamEmpty,
                    firstMessageInRequest?.id ?: startMessageId,
                    if (!isStreamEmpty && firstMessageInRequest?.timestamp != null)
                        firstMessageInRequest!!.timestamp
                    else {
                        lastTimestamp
                    }
                )
            )
            delay(sendEmptyDelay)
        }
    }


    override suspend fun processMessage() {
        coroutineScope {
            launch { emptySender(this) }

            initialize()

            while (isActive && needLoadMessage) {

                tryToReloadStream()

                val messageBatches = loadMoreMessage()

                for (parsedMessage in messageBatches) {
                    logger.trace { parsedMessage.messageBatch.id }
                    sendToChannel(PipelineRawBatchData(isStreamEmpty, lastElement, lastTimestamp, parsedMessage))
                }

                changeStreamState(messageBatches)

                if (isStreamEmpty) {
                    delay(sseSearchDelay)
                }
            }
        }
    }
}


