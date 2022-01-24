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
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatchData
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.handlers.PipelineComponent
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.time.Instant


class MessageContinuousStream(
    private val startMessageId: StoredMessageId?,
    private val initializer: StreamInitializer,
    private val startTimestamp: Instant,
    externalScope: CoroutineScope,
    messageFlowCapacity: Int
) : PipelineComponent(
    startMessageId,
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

    private val sendEmptyDelay = context.configuration.sendEmptyDelay.value.toLong()

    private var firstPull: Boolean = true
    private var isStreamEmpty: Boolean = false

    private var lastElement: StoredMessageId? = null
    private var lastTimestamp: Instant = startTimestamp


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

    private fun markStreamEmpty() {
        isStreamEmpty = true
        lastTimestamp = if (searchRequest.searchDirection == TimeRelation.AFTER) {
            Instant.MAX
        } else {
            Instant.MIN
        }
    }

    private fun getLastElementInBatch(batchWrapper: MessageBatchWrapper): StoredMessage {
        return if (searchRequest.searchDirection == TimeRelation.AFTER) {
            batchWrapper.messageBatch.lastMessage
        } else {
            batchWrapper.messageBatch.firstMessage
        }
    }


    private suspend fun getMessagesSequence(): Sequence<MessageBatchWrapper> {
        if (lastElement == null || messageLoader == null) {
            return emptySequence()
        }
        return messageLoader!!.pullMoreMessage(lastElement!!, firstPull)
    }


    private suspend fun emptySender(parentScope: CoroutineScope) {
        while (parentScope.isActive) {
            sendToChannel(
                EmptyPipelineObject(isStreamEmpty, lastElement ?: startMessageId, lastTimestamp)
            )
            delay(sendEmptyDelay)
        }
    }


    override suspend fun processMessage() {
        coroutineScope {
            launch { emptySender(this) }

            initialize()

            val messageBatches = getMessagesSequence()

            for (parsedMessage in messageBatches) {
                logger.trace { parsedMessage.messageBatch.id }

                getLastElementInBatch(parsedMessage).also {
                    lastElement = it.id
                    lastTimestamp = it.timestamp
                }

                sendToChannel(PipelineRawBatchData(isStreamEmpty, lastElement, lastTimestamp, parsedMessage))
            }

            markStreamEmpty()
        }
    }
}


