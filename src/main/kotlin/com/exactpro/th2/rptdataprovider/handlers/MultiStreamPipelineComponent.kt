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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel

abstract class MultiStreamPipelineComponent<B, G, RM, PM>(
    val context: Context<B, G, RM, PM>,
    val searchRequest: SseMessageSearchRequest<RM, PM>,
    val externalScope: CoroutineScope,
    protected val streamNames: Set<String> = emptySet(),
    val previousComponent: MultiStreamPipelineComponent<B, G, RM, PM>? = null,
    messageFlowCapacity: Int
) {
    private val messageFlow = Channel<PipelineStepObject>(messageFlowCapacity)

/*
    // TODO: we can avoid nested iteration
    protected val streams: MutableMap<String, StreamPointer?> = commonStreamNames.asSequence()
        .map { streamName ->
            val resumeFromId = searchRequest.resumeFromIdsList.asSequence()
                .filter { it.stream.name == streamName }
                .firstResumeId { it.timestamp }
            Pair(streamName, resumeFromId)
        }
        .toMap()
        .toMutableMap()

    protected val streams2: MutableMap<String, StreamPointer?> = resumeFromId
        .filter { entry -> entry.value.stream.name in commonStreamNames }
        .
    commonStreamNames.asSequence()
    val resumeFromId: Map<String, StreamPointer?> = request.resumeFromIdsList.asSequence()
        .filter { it.stream.name in streams }
        .groupBy { streamPointer -> streamPointer.stream.name }
        .mapValues { entry -> entry.value.streamPointerSelector { it.timestamp } }
*/



    protected var processedMessagesCounter: Long = 0

    val processedMessageCount
        get() = processedMessagesCounter

    protected abstract suspend fun processMessage()

    protected suspend fun sendToChannel(message: PipelineStepObject) {
        LOGGER.trace { "${this::class.simpleName} sending: $messageFlow" }
        messageFlow.send(message)
        LOGGER.trace { "${this::class.simpleName} sent ${message::class.simpleName}: $messageFlow" }
    }

    suspend fun pollMessage(): PipelineStepObject {
        LOGGER.trace { "${this::class.simpleName} receiving: $messageFlow" }
        return messageFlow.receive().also {
            LOGGER.trace { "${this::class.simpleName} received ${it::class.simpleName}: $messageFlow" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}