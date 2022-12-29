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
import com.exactpro.th2.rptdataprovider.entities.internal.*
import com.exactpro.th2.rptdataprovider.entities.mappers.TimeRelationMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedMessageInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.messages.ChainBuilder
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageExtractor
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import io.prometheus.client.Counter
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
        private val searchMessageRequests =
            Counter.build("th2_search_messages", "Count of search message requests")
                .register()
    }


    @OptIn(ExperimentalTime::class)
    suspend fun searchMessagesSse(request: SseMessageSearchRequest, writer: StreamWriter) {
        withContext(coroutineContext) {

            searchMessageRequests.inc()

            val lastMessageIdCounter = AtomicLong(0)
            val pipelineStatus = PipelineStatus(context = applicationContext)
            var streamMerger: StreamMerger? = null

            val chainScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))

            flow {
                streamMerger = ChainBuilder(applicationContext, request, chainScope, pipelineStatus).buildChain()

                do {
                    logger.trace { "Polling message from pipeline" }
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

    suspend fun getIds(request: SseMessageSearchRequest): Map<String, List<StreamInfo>> {
        searchMessageRequests.inc()
        val resumeId = request.resumeFromIdsList.firstOrNull()
        val messageId = resumeId?.let {
            StoredMessageId(
                it.stream.bookId,
                it.stream.name,
                it.stream.direction,
                it.timestamp,
                it.sequence
            )
        }
        val resultRequest = resumeId?.let {
            request.copy(startTimestamp = resumeId.timestamp)
        } ?: request

        val before = getIds(resultRequest, messageId, TimeRelation.BEFORE)
        val after = getIds(resultRequest, messageId, TimeRelation.AFTER)

        return mapOf(
            TimeRelationMapper.toHttp(TimeRelation.BEFORE) to before,
            TimeRelationMapper.toHttp(TimeRelation.AFTER) to after,
        )
    }

    private suspend fun getIds(
        request: SseMessageSearchRequest,
        messageId: StoredMessageId?,
        searchDirection: TimeRelation
    ): MutableList<StreamInfo> {
        val resultRequest = request.copy(searchDirection = searchDirection)

        val pipelineStatus = PipelineStatus(context = applicationContext)

        val streamNames = resultRequest.stream.flatMap { stream ->
            Direction.values().map { StreamName(stream, it, request.bookId) }
        }

        val coroutineScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))
        pipelineStatus.addStreams(streamNames.map { it.toString() })

        val streamInfoList = mutableListOf<StreamInfo>()

        val extractors = streamNames.map { streamName ->
            MessageExtractor(
                applicationContext,
                resultRequest,
                streamName,
                coroutineScope,
                1,
                pipelineStatus
            )
        }

        extractors.forEach { messageExtractor ->
            val listPair = mutableListOf<Pair<StoredMessageId?, Boolean>>()

            do {
                messageExtractor.pollMessage().let {
                    if (it is PipelineRawBatch && listPair.isEmpty()) {
                        val trimmedMessages = it.storedBatchWrapper.trimmedMessages
                        for (trimmedMessage in trimmedMessages) {
                            if (trimmedMessage.id == messageId) continue
                            if (listPair.isNotEmpty()) break
                            listPair.add(Pair(trimmedMessage.id, it.streamEmpty))
                        }
                    } else if (listPair.isEmpty() && it.streamEmpty) {
                        listPair.add(Pair(null, it.streamEmpty))
                    }
                }
            } while (listPair.isEmpty())

            listPair.first().let {
                streamInfoList.add(StreamInfo(messageExtractor.streamName!!, it.first))
            }
        }
        return streamInfoList
    }
}