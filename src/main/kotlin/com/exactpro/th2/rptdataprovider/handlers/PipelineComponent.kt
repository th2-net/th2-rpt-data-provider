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

import com.exactpro.th2.rptdataprovider.entities.internal.MessagePipelineStepObject
import com.exactpro.th2.rptdataprovider.entities.internal.RawBatch
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch


data class BatchPair<T, Y>(val previous: T, val new: Y)


abstract class PipelineComponent<T : MessagePipelineStepObject, Y : MessagePipelineStepObject>(
    private val externalScope: CoroutineScope,
    val previousComponent: PipelineComponent<*, T>? = null
) {
    private val messageFlow = Channel<Y>(Channel.BUFFERED)

    init {
        externalScope.launch {
            processMessage()
        }
    }

    protected abstract suspend fun processMessage()

    protected suspend fun sendToChannel(message: Y) {
        messageFlow.send(message)
    }

    suspend fun pollMessage(): Y {
        return messageFlow.receive()
    }
}