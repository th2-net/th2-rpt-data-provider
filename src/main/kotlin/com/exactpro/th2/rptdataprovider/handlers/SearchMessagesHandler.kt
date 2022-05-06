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


    @OptIn(ExperimentalTime::class, ExperimentalCoroutinesApi::class)
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
    suspend fun getIds(request: SseMessageSearchRequest): Map<String, List<StreamInfo>> {

        searchMessageRequests.inc()

        val resumeId = request.resumeFromIdsList.firstOrNull()

        val message = resumeId?.let {
            applicationContext.cradleService.getMessageSuspend(
                StoredMessageId(
                    it.streamName,
                    it.direction,
                    it.sequence
                )
            )
        }

        val resultRequest = message?.let {
            request.copy(startTimestamp = message.timestamp)
        } ?: request

        val pipelineStatus = PipelineStatus(context = applicationContext)

        val streamNames = resultRequest.stream.flatMap { stream ->
            Direction.values().map { StreamName(stream, it) }
        }

        val coroutineScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))
        pipelineStatus.addStreams(streamNames.map { it.toString() })

        val streamInfoMap = mutableMapOf<TimeRelation, MutableList<StreamInfo>>()
        streamInfoMap[TimeRelation.AFTER] = mutableListOf()
        streamInfoMap[TimeRelation.BEFORE] = mutableListOf()

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
                    if (it is PipelineRawBatch && listPair.size < 2) {
                        val trimmedMessages = it.storedBatchWrapper.trimmedMessages
                        for (trimmedMessage in trimmedMessages) {
                            if (trimmedMessage == message) continue
                            if (listPair.size == 2) break
                            listPair.add(Pair(trimmedMessage.id, it.streamEmpty))
                        }
                    } else if (listPair.size < 2 && it.streamEmpty) {
                        val name = messageExtractor.streamName!!
                        listPair.add(Pair(StoredMessageId(name.name, name.direction, -1), it.streamEmpty))
                    }
                }
            } while (listPair.size < 2)

            listPair.first().let {
                streamInfoMap
                    .getValue(TimeRelation.AFTER)
                    .add(StreamInfo(messageExtractor.streamName!!, it.first))
            }
            listPair.last().let {
                streamInfoMap
                    .getValue(TimeRelation.BEFORE)
                    .add(StreamInfo(messageExtractor.streamName!!, it.first))
            }
        }

        return streamInfoMap.entries.associate { TimeRelationMapper.toHttp(it.key) to it.value }
    }
}