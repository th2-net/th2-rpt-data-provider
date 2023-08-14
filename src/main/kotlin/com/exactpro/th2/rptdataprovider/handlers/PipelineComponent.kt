/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.rptdataprovider.entities.internal.StreamName
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging


abstract class PipelineComponent<B, G, RM, PM>(
    val context: Context<B, G, RM, PM>,
    val searchRequest: SseMessageSearchRequest<RM, PM>,
    val externalScope: CoroutineScope,
    val streamName: StreamName? = null,
    val previousComponent: PipelineComponent<B, G, RM, PM>? = null,
    messageFlowCapacity: Int
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val messageFlow = Channel<PipelineStepObject>(messageFlowCapacity)
    protected var processedMessagesCounter: Long = 0

    val processedMessageCount
        get() = processedMessagesCounter


    protected abstract suspend fun processMessage()


    protected suspend fun sendToChannel(message: PipelineStepObject) {
        logger.trace { "${this::class.simpleName} sending: $messageFlow" }
        messageFlow.send(message)
        logger.trace { "${this::class.simpleName} sent ${message::class.simpleName}: $messageFlow" }
    }


    suspend fun pollMessage(): PipelineStepObject {
        logger.trace { "${this::class.simpleName} receiving: $messageFlow" }
        return messageFlow.receive().also {
            logger.trace { "${this::class.simpleName} received ${it::class.simpleName}: $messageFlow" }
        }
    }
}
