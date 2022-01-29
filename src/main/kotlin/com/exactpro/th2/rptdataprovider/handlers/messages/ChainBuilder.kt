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

import com.exactpro.cradle.Direction
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.PipelineStatus
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.CoroutineScope

class ChainBuilder(
    private val context: Context,
    private val request: SseMessageSearchRequest,
    private val externalScope: CoroutineScope,
    private val pipelineStatus: PipelineStatus
) {

    private val messageContinuousStreamBuffer = context.configuration.messageContinuousStreamBuffer.value.toInt()
    private val messageDecoderBuffer = context.configuration.messageDecoderBuffer.value.toInt()
    private val messageFilterBuffer = context.configuration.messageFilterBuffer.value.toInt()
    private val messageStreamMergerBuffer = context.configuration.messageStreamMergerBuffer.value.toInt()


    fun buildChain(): StreamMerger {
        val streamNames = request.stream.flatMap { stream -> Direction.values().map { StreamName(stream, it) } }

        val dataStreams = streamNames.map { streamName ->
            pipelineStatus.addStream(streamName.toString())

            val messageExtractor = MessageExtractor(
                context,
                request,
                streamName,
                externalScope,
                messageContinuousStreamBuffer,
                pipelineStatus
            )

            val messageBatchConverter = MessageBatchConverter(messageExtractor, 10, pipelineStatus)
            val messageBatchDecoder = MessageBatchDecoder(messageBatchConverter, 100, pipelineStatus)
            val messageBatchUnpacker = MessageBatchUnpacker(messageBatchDecoder, 100, pipelineStatus)

            MessageFilter(messageBatchUnpacker, messageFilterBuffer, pipelineStatus)
        }

        return StreamMerger(context, request, externalScope, dataStreams, messageStreamMergerBuffer, pipelineStatus)
    }
}
