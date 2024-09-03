/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.ProtoRawMessage
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import com.exactpro.th2.rptdataprovider.TransportRawMessage
import com.exactpro.th2.rptdataprovider.entities.internal.CommonStreamName
import com.exactpro.th2.rptdataprovider.entities.internal.EmptyPipelineObject
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineFilteredMessage
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineKeepAlive
import com.exactpro.th2.rptdataprovider.entities.internal.PipelineRawBatch
import com.exactpro.th2.rptdataprovider.entities.internal.StreamEndObject
import com.exactpro.th2.rptdataprovider.entities.mappers.TimeRelationMapper
import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.responses.StreamInfo
import com.exactpro.th2.rptdataprovider.entities.sse.LastScannedMessageInfo
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.messages.ChainBuilder
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageExtractor
import com.exactpro.th2.rptdataprovider.handlers.messages.ProtoChainBuilder
import com.exactpro.th2.rptdataprovider.handlers.messages.StreamMerger
import com.exactpro.th2.rptdataprovider.handlers.messages.TransportChainBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.client.Counter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.withContext
import java.time.temporal.ChronoUnit.DAYS
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

abstract class SearchMessagesHandler<B, G, RM, PM>(
    protected val applicationContext: Context<B, G, RM, PM>
) {

    companion object {
        private val logger = KotlinLogging.logger { }
        private val searchMessageRequests =
            Counter.build("th2_search_messages", "Count of search message requests")
                .register()
    }


    @OptIn(ExperimentalTime::class)
    suspend fun searchMessagesSse(request: SseMessageSearchRequest<RM, PM>, writer: StreamWriter<RM, PM>) {
        withContext(coroutineContext) {

            searchMessageRequests.inc()

            val lastMessageIdCounter = AtomicLong(0)
            val pipelineStatus = PipelineStatus()
            var streamMerger: StreamMerger<B, G, RM, PM>? = null

            val chainScope = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))

            flow {
                streamMerger = chainBuilder(request, chainScope, pipelineStatus).buildChain()

                do {
                    logger.trace { "Polling message from pipeline" }
                    val message = measureTimedValue {
                        streamMerger?.pollMessage()
                    }

                    measureTimeMillis {
                        message.value?.let { emit(it) }
                    }.also {
                        if (message.value !is EmptyPipelineObject) {
                            logger.trace { "message was produced in ${
                                message.duration.toDouble(DurationUnit.MILLISECONDS).roundToInt()
                            } and consumed in ${it}ms" }
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

                    if (it is PipelineFilteredMessage<*, *>) {
                        pipelineStatus.countSend()
                        writer.write(it as PipelineFilteredMessage<RM, PM>, lastMessageIdCounter)
                    } else if (it is PipelineKeepAlive) {
                        writer.write(LastScannedMessageInfo(it), lastMessageIdCounter)
                    }
                }
        }
    }

    suspend fun getIds(request: SseMessageSearchRequest<RM, PM>, lookupLimitDays: Long): Map<String, List<StreamInfo>> {
        require(request.startTimestamp != null && request.endTimestamp == null) {
            "startTimestamp must be not null and endTimestamp be null in request: $request"
        }
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

        val before = getIds(resultRequest, messageId, lookupLimitDays, BEFORE)
        val after = getIds(resultRequest, messageId, lookupLimitDays, AFTER)

        return mapOf(
            TimeRelationMapper.toHttp(BEFORE) to before,
            TimeRelationMapper.toHttp(AFTER) to after,
        )
    }

    protected abstract fun chainBuilder(
        request: SseMessageSearchRequest<RM, PM>,
        chainScope: CoroutineScope,
        pipelineStatus: PipelineStatus
    ): ChainBuilder<B, G, RM, PM>

    private suspend fun getIds(
        request: SseMessageSearchRequest<RM, PM>,
        messageId: StoredMessageId?,
        lookupLimitDays: Long,
        searchDirection: TimeRelation
    ): MutableList<StreamInfo> {
        val lookupLimit = request.lookupLimitDays ?: lookupLimitDays
        val resultRequest = request.run {
            copy(
                searchDirection = searchDirection,
                endTimestamp = when (searchDirection) {
                    BEFORE -> startTimestamp?.minus(lookupLimit, DAYS)
                    AFTER -> startTimestamp?.plus(lookupLimit, DAYS)
                }
            ).also(SseMessageSearchRequest<*, *>::checkIdsRequest)
        }

        val pipelineStatus = PipelineStatus()

        val streamNames = resultRequest.stream.map { stream -> CommonStreamName(request.bookId, stream) }

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
                streamInfoList.add(StreamInfo(messageExtractor.commonStreamName, it.first))
            }
        }
        return streamInfoList
    }
}

class ProtoSearchMessagesHandler(
    applicationContext: Context<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message>
): SearchMessagesHandler<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message> (
    applicationContext
) {
    override fun chainBuilder(
        request: SseMessageSearchRequest<ProtoRawMessage, Message>,
        chainScope: CoroutineScope,
        pipelineStatus: PipelineStatus
    ): ChainBuilder<MessageGroupBatch, ProtoMessageGroup, ProtoRawMessage, Message> =
        ProtoChainBuilder(applicationContext, request, chainScope, pipelineStatus)
}
class TransportSearchMessagesHandler(
    applicationContext: Context<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage>
): SearchMessagesHandler<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage> (
    applicationContext
) {
    override fun chainBuilder(
        request: SseMessageSearchRequest<TransportRawMessage, ParsedMessage>,
        chainScope: CoroutineScope,
        pipelineStatus: PipelineStatus
    ): ChainBuilder<GroupBatch, TransportMessageGroup, TransportRawMessage, ParsedMessage> =
        TransportChainBuilder(applicationContext, request, chainScope, pipelineStatus)

}
