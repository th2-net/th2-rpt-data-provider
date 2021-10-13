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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.dayStart
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.*
import java.lang.Integer.min
import java.time.Instant


class MessageContinuousStream(
    private val messageLoader: MessageLoader,
    private val startMessageId: StoredMessageId?,
    private val initializer: StreamInitializer,
    private val startTimestamp: Instant,
    externalScope: CoroutineScope
) : PipelineComponent(initializer.context, initializer.request, externalScope, initializer.stream) {


    private val sseSearchDelay = context.configuration.sseSearchDelay.value.toLong()
    private val maxMessagesLimit = context.configuration.maxMessagesLimit.value.toInt()
    private var perStreamLimit = min(maxMessagesLimit, searchRequest.resultCountLimit ?: 25)

    private var firstPull: Boolean = true
    private var isStreamEmpty: Boolean = false
    private var needLoadMessage: Boolean = true

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant = startTimestamp


    private suspend fun initialize() {
        if (startMessageId == null) {
            initializer.initStream(startTimestamp)?.let {
                lastElement = it.id
                lastTimestamp = it.timestamp
            }
        } else {
            lastElement = startMessageId
            firstPull = false
        }
    }


    private suspend fun tryToLoadStreamAfter() {
        if (lastElement == null) {
            val currentTimestamp = Instant.now()
            initializer.tryToGetStartId(currentTimestamp.dayStart())?.let {
                lastElement = it
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


    private fun changeStreamMessageIndex(filteredIdsList: List<MessageBatchWrapper>): Pair<StoredMessageId?, Instant> {
        val batchWrapper = filteredIdsList.lastOrNull { !it.messageBatch.isEmpty }
        return if (batchWrapper == null) {
            lastElement to lastTimestamp
        } else {
            val lastMessage =
                if (searchRequest.searchDirection == TimeRelation.AFTER) {
                    batchWrapper.messageBatch.firstMessage
                } else {
                    batchWrapper.messageBatch.lastMessage
                }
            lastMessage.id to lastMessage.timestamp
        }
    }


    private suspend fun loadMoreMessage(): List<MessageBatchWrapper> {
        if (lastElement == null || !needLoadMessage) {
            return emptyList()
        }
        return messageLoader.pullMoreMessage(lastElement!!, firstPull, perStreamLimit).also { messages ->
            changeStreamMessageIndex(messages).let {
                lastElement = it.first
                lastTimestamp = it.second
            }

            firstPull = false
            isStreamEmpty = messages.size < perStreamLimit
            perStreamLimit = min(perStreamLimit * 2, maxMessagesLimit)
        }
    }


    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            lastElement?.let {
                sendToChannel(EmptyPipelineObject(isStreamEmpty, lastElement, lastTimestamp))
                delay(sseSearchDelay)
            }
        }
    }


    override suspend fun processMessage() {
        coroutineScope {

            initialize()
            launch { emptySender(this) }

            while (isActive && needLoadMessage) {

                tryToReloadStream()

                val messageBatches = loadMoreMessage()

                for (parsedMessage in messageBatches) {
                    sendToChannel(PipelineRawBatchData(isStreamEmpty, lastElement, lastTimestamp, parsedMessage))
                }

                if (isStreamEmpty) {
                    delay(sseSearchDelay)
                }
            }
        }
    }
}


