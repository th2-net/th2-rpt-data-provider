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
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.filters.FilterPredicate
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedMessageInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.messages.ChainBuilder
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageExtractor
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.takeWhile
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class SearchMessagesHandler(private val applicationContext: Context) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    private val pipelineInfoSendDelay = applicationContext.configuration.pipelineInfoSendDelay.value.toLong()
    private val sendPipelineStatus = applicationContext.configuration.sendPipelineStatus.value.toBoolean()

    private suspend fun sendPipelineStatus(
        pipelineStatus: PipelineStatus,
        writer: StreamWriter,
        coroutineScope: CoroutineScope,
        lastMessageIdCounter: AtomicLong
    ) {
        while (coroutineScope.isActive) {
            writer.write(pipelineStatus.getSnapshot(), lastMessageIdCounter)
            delay(pipelineInfoSendDelay)
        }
    }

    @OptIn(ExperimentalTime::class, ExperimentalCoroutinesApi::class)
    suspend fun searchMessagesSse(request: SseMessageSearchRequest, writer: StreamWriter) {
        withContext(coroutineContext) {
            val lastMessageIdCounter = AtomicLong(0)
            val pipelineStatus = PipelineStatus(context = applicationContext)
            var streamMerger: StreamMerger? = null
            val chainScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))

            flow {
                streamMerger = ChainBuilder(applicationContext, request, chainScope, pipelineStatus).buildChain()

                if (sendPipelineStatus) {
                    launch {
                        sendPipelineStatus(pipelineStatus, writer, chainScope, lastMessageIdCounter)
                    }
                }

                do {
                    val message = measureTimedValue {
                        streamMerger?.pollMessage()
                    }

                    measureTimeMillis {
                        message.value?.let { emit(it) }
                    }.also {
                        if (message.value !is EmptyPipelineObject) {
                            logger.trace { "message was produced in ${message.duration.inMilliseconds.roundToInt()} and consumed in ${it}ms" }
                        }
                    }

                } while (true)
            }
                .takeWhile { it !is StreamEndObject }
                .onCompletion {
                    streamMerger?.let { merger -> writer.write(merger.getStreamsInfo()) }
                    chainScope.cancel()
                    it?.let { throwable -> throw throwable }
                    logger.debug { "message pipeline flow has been completed" }
                }
                .collect {
                    coroutineContext.ensureActive()

                    if (it is PipelineFilteredMessage) {
                        pipelineStatus.countSend()
                        writer.write(it, lastMessageIdCounter)
                    } else if (it is PipelineKeepAlive) {
                        writer.write(LastScannedMessageInfo(it), lastMessageIdCounter)
                    }
                }
        }
    }

    @OptIn(ExperimentalTime::class, InternalCoroutinesApi::class)
    suspend fun getIds(requests: List<SseMessageSearchRequest>): Map<String, List<StreamInfo>> {
        val pipelineStatus = PipelineStatus(context = applicationContext)

        val streamNames = requests.first().stream.flatMap { stream ->
            Direction.values().map { StreamName(stream, it) }
        }

        val coroutineScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))
        pipelineStatus.addStreams(streamNames.map { it.toString() })

        val streamInfoList = mutableMapOf<TimeRelation, MutableList<StreamInfo>>()
        streamInfoList[TimeRelation.AFTER] = mutableListOf()
        streamInfoList[TimeRelation.BEFORE] = mutableListOf()

        val extractors = streamNames.flatMap { streamName ->
            requests.map { request ->
                MessageExtractor(
                    applicationContext,
                    request,
                    streamName,
                    coroutineScope,
                    1,
                    pipelineStatus
                )
            }
        }

        extractors.forEach { messageExtractor ->
            var pair: Pair<StoredMessageId?, Boolean>

            do {
                messageExtractor.pollMessage().let {
                    pair = if (it is PipelineRawBatch) {
                        val storedMessage = it.storedBatchWrapper.trimmedMessages.firstOrNull()
                        Pair(storedMessage?.id, it.streamEmpty)
                    } else {
                        Pair(null, it.streamEmpty)
                    }
                }
            } while (pair.first == null && !pair.second)

            pair.first?.let {
                streamInfoList
                    .getValue(messageExtractor.request.searchDirection)
                    .add(StreamInfo(messageExtractor.streamName!!, it))
            }
        }

        return streamInfoList.entries.associate { it.key.label to it.value }
    }
}