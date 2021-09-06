/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedMessageInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedObjectInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageStreamProducer
import com.fasterxml.jackson.databind.ObjectMapper
import io.prometheus.client.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

class SearchMessagesHandler(private val context: Context) {

    private val messageSearchPipelineBuffer = context.configuration.messageSearchPipelineBuffer.value.toInt()

    companion object {
        private val logger = KotlinLogging.logger { }

        private val processedMessageCount = Counter.build(
            "processed_message_count", "Count of processed Message"
        ).register()
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun searchMessagesSse(
        request: SseMessageSearchRequest,
        jacksonMapper: ObjectMapper,
        keepAlive: suspend (StreamWriter, lastScannedObjectInfo: LastScannedObjectInfo, counter: AtomicLong) -> Unit,
        writer: StreamWriter
    ) {
        withContext(coroutineContext) {
            val startMessageCountLimit = 25
            val lastScannedObject = LastScannedMessageInfo()
            val lastEventId = AtomicLong(0)
            val scanCnt = AtomicLong(0)
            var lastIdInStream: MutableMap<Pair<String, Direction>, StoredMessageId?> = mutableMapOf()

            var streamProducer: MessageStreamProducer? = null

            flow {
                streamProducer = MessageStreamProducer.create(request, context, this@withContext)

                streamProducer
                    ?.getMessageStream()
                    ?.collect { emit(it) }
            }.map {
                async {
                    val parsedMessage = it.getParsedMessage()
                    context.messageCache.put(parsedMessage.messageId, parsedMessage)
                    MessageWithMetadata(parsedMessage).apply {
                        finalFiltered = request.filterPredicate.apply(this)
                    }
                }.also { coroutineContext.ensureActive() }
            }
                .buffer(messageSearchPipelineBuffer)
                .map { it.await() }
                .onEach { (message, _) ->
                    lastScannedObject.update(message, scanCnt)
                    message.id.let {
                        lastIdInStream[Pair(it.streamName, it.direction)] = it
                    }
                    processedMessageCount.inc()
                }
                .filter { it.finalFiltered }
                .let { fl -> request.resultCountLimit?.let { fl.take(it) } ?: fl }
                .onStart {
                    launch {
                        keepAlive.invoke(writer, lastScannedObject, lastEventId)
                    }
                }
                .onCompletion {
                    writer.write(lastIdInStream)
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()
                    writer.write(it as MessageWithMetadata, lastEventId)
                }
        }
    }
}
