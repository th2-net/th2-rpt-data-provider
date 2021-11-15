﻿/*******************************************************************************
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
import com.exactpro.th2.rptdataprovider.dayStart
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.lang.Integer.min
import java.time.Instant


class MessageContinuousStream(
    private val startMessageId: StoredMessageId?,
    private val initializer: StreamInitializer,
    private val startTimestamp: Instant,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int
) : PipelineComponent(
    initializer.context,
    initializer.request,
    externalScope,
    initializer.stream,
    messageFlowCapacity = messageFlowCapacity
) {

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


    private data class StreamState(
        val firstMessageInRequest: StoredMessage?,
        val lastMessageInRequest: StoredMessageId?,
        val lastTimestamp: Instant
    )

    init {
        externalScope.launch {
            processMessage()
        }
    }

    private suspend fun initialize() {
        if (startMessageId == null) {
            initializer.initStream(startTimestamp)?.let {
                lastElement = it.id
                lastTimestamp = it.timestamp
            }
        } else {
            context.cradleService.getMessageSuspend(startMessageId)?.let {
                lastElement = it.id
                lastTimestamp = it.timestamp
                firstPull = false
            }
        }
        messageLoader = MessageLoader(context, startTimestamp, searchRequest.searchDirection)
    }

    private suspend fun tryToLoadStreamAfter() {
        if (lastElement == null || isStreamEmpty) {
            val currentTimestamp = Instant.now()
            if (lastElement == null) {
                initializer.tryToGetStartId(currentTimestamp.dayStart())?.let {
                    lastElement = it
                }
            }
            lastTimestamp = currentTimestamp
        }
    }

    private fun tryToLoadStreamBefore() {
        if (lastElement == null || isStreamEmpty) {
            lastTimestamp = Instant.MIN
            needLoadMessage = false
        }
    }

    private suspend fun tryToReloadStream() {
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
        return messageLoader!!.pullMoreMessage(lastElement!!, firstPull, perStreamLimit).also {
            firstPull = false
            perStreamLimit = min(perStreamLimit * 2, maxMessagesLimit)
        }
    }

    private fun getEmptyMessage(): EmptyPipelineObject {
        return EmptyPipelineObject(
            isStreamEmpty,
            firstMessageInRequest?.id,
            if (!isStreamEmpty && firstMessageInRequest?.timestamp != null)
                firstMessageInRequest!!.timestamp
            else {
                lastTimestamp
            }
        )
    }

    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            sendToChannel(getEmptyMessage())
            delay(sendEmptyDelay)
        }
    }

    private fun makeRawBatchData(message: MessageBatchWrapper): PipelineRawBatchData {
        return PipelineRawBatchData(isStreamEmpty, lastElement, lastTimestamp, message)
    }

    override suspend fun processMessage() {
        coroutineScope {

            sendToChannel(getEmptyMessage())
            launch { emptySender(this) }

            initialize()

            while (isActive && needLoadMessage) {

                tryToReloadStream()

                val messageBatches = loadMoreMessage()

                for (message in messageBatches) {
                    logger.trace { message.messageBatch.id }
                    sendToChannel(makeRawBatchData(message))
                }

                changeStreamState(messageBatches)

                if (isStreamEmpty) {
                    delay(sseSearchDelay)
                }
            }
        }
    }
}