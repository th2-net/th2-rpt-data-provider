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

import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedMessageInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.messages.ChainBuilder
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext

class SearchMessagesHandler(private val context: Context) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    @InternalCoroutinesApi
    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun searchMessagesSse(request: SseMessageSearchRequest, writer: StreamWriter) {
        withContext(coroutineContext) {
            val lastMessageIdCounter = AtomicLong(0)
            val pipelineStatus = PipelineStatus(streams = mutableMapOf());
            var streamMerger: StreamMerger? = null

            flow {
                streamMerger = ChainBuilder(context, request, this@withContext, pipelineStatus).buildChain()

                do {
                    val message = streamMerger?.pollMessage()
                    logger.trace { message?.lastProcessedId }
                    message?.let { emit(it) }
                } while (true)
            }
                .takeWhile { it !is StreamEndObject }
                .onCompletion {
                    streamMerger?.let { merger -> writer.write(merger.getStreamsInfo()) }
                    coroutineContext.cancelChildren()
                    it?.let { throwable -> throw throwable }
                }
                .collect {
                    coroutineContext.ensureActive()

                    logger.trace { it.lastProcessedId }
                    if (it is PipelineFilteredMessage) {
                        logger.trace { it.lastProcessedId }
                        writer.write(it.payload, lastMessageIdCounter)
                    } else if (it is PipelineKeepAlive) {
                        logger.trace { it.lastProcessedId }
                        writer.write(LastScannedMessageInfo(it), lastMessageIdCounter)
                    }
                    writer.write(pipelineStatus, lastMessageIdCounter)
                }
        }
    }
}
