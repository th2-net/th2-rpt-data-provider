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
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.handlers.StreamName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.InternalCoroutinesApi
import java.time.Instant

class ChainBuilder(
    private val context: Context,
    private val request: SseMessageSearchRequest,
    private val externalScope: CoroutineScope
) {

    private val messageContinuousStreamBuffer = context.configuration.messageContinuousStreamBuffer.value.toInt()
    private val messageDecoderBuffer = context.configuration.messageDecoderBuffer.value.toInt()
    private val messageFilterBuffer = context.configuration.messageFilterBuffer.value.toInt()
    private val messageStreamMergerBuffer = context.configuration.messageStreamMergerBuffer.value.toInt()

    private suspend fun getTimestampFromResumeIds(request: SseMessageSearchRequest): Instant? {
        val resumeTimestamps = request.resumeFromIdsList
            .mapNotNull { context.cradleService.getMessageSuspend(it)?.timestamp }

        return if (request.searchDirection == TimeRelation.AFTER) {
            resumeTimestamps.minBy { it }
        } else {
            resumeTimestamps.maxBy { it }
        }
    }

    private suspend fun chooseStartTimestamp(request: SseMessageSearchRequest): Instant {
        return request.startTimestamp
            ?: getTimestampFromResumeIds(request)
            ?: Instant.now()
    }

    private fun getRequestStreamNames(request: SseMessageSearchRequest): List<StreamName> {
        return request.stream
            .flatMap { stream -> Direction.values().map { StreamName(stream, it) } }
    }

    private fun getRequestResumeId(request: SseMessageSearchRequest): Map<StreamName, StoredMessageId?> {
        return request.resumeFromIdsList
            .associateBy { StreamName(it.streamName, it.direction) }
    }

    @InternalCoroutinesApi
    suspend fun buildChain(): StreamMerger {

        val streamNames = getRequestStreamNames(request)
        val resumeFromIds = getRequestResumeId(request)
        val startTimestamp = chooseStartTimestamp(request)

        val dataStreams = streamNames.map { streamName ->
            val streamInitializer = StreamInitializer(context, request, streamName)

            val messageStream = MessageContinuousStream(
                resumeFromIds[streamName],
                streamInitializer,
                startTimestamp,
                externalScope,
                messageContinuousStreamBuffer
            )

            val messageDecoder = MessageDecoder(messageStream, messageDecoderBuffer)

            MessageFilter(messageDecoder, messageFilterBuffer)
        }

        return StreamMerger(context, request, externalScope, dataStreams, messageStreamMergerBuffer)
    }
}