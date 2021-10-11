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

package com.exactpro.th2.rptdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

data class StreamName(val name: String, val direction: Direction)

abstract class PipelineComponent(
    val context: Context,
    val searchRequest: SseMessageSearchRequest,
    val streamName: StreamName,
    val externalScope: CoroutineScope,
    val previousComponent: PipelineComponent? = null
) {
    private val messageFlow = Channel<PipelineStepObject>(Channel.BUFFERED)
    open var processedMessages: Long = 0

    init {
        externalScope.launch {
            processMessage()
        }
    }

    protected abstract suspend fun processMessage()


    protected suspend fun sendToChannel(message: PipelineStepObject) {
        messageFlow.send(message)
    }
    

    suspend fun pollMessage(): PipelineStepObject {
        return messageFlow.receive()
    }
}