/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.services.rabbitmq

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.proto
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.rptdataprovider.ProtoMessageGroup
import com.exactpro.th2.rptdataprovider.TransportMessageGroup
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import mu.KotlinLogging


data class CodecRequestId(private val ids: Set<BaseMessageId>) {

    data class BaseMessageId(val sessionAlias: String, val direction: Direction, val sequence: Long) {
        constructor(messageId: MessageID) : this(
            messageId.connectionId.sessionAlias,
            messageId.direction,
            messageId.sequence
        )

        constructor(messageId: MessageId) : this(
            messageId.sessionAlias,
            messageId.direction.proto,
            messageId.sequence
        )

        override fun toString(): String {
            return "$sessionAlias:$direction:$sequence"
        }

    }
}


abstract class CodecBatchRequest<B, G, PM>(
    val streamName: String
) {
    abstract val rawMessageBatch: B
    abstract val requestId: CodecRequestId
    abstract val firstSequence: Long
    abstract val lastSequence: Long
    abstract val groupsCount: Int
    abstract val sessionAlias: String
    abstract val protocol: String?
    abstract val stream: String

    val requestHash: Int
        get() = requestId.hashCode()


    fun toPending(): PendingCodecBatchRequest<G, PM> {
        return PendingCodecBatchRequest(CompletableDeferred(), streamName)
    }
}

class ProtoCodecBatchRequest(
    override val rawMessageBatch: MessageGroupBatch,
    streamName: String
): CodecBatchRequest<MessageGroupBatch, ProtoMessageGroup, Message>(streamName) {
    override val requestId = fromMessageGroupBatch(rawMessageBatch)
    override val firstSequence: Long = rawMessageBatch.firstSequence()
    override val lastSequence: Long = rawMessageBatch.lastSequence()
    override val groupsCount: Int = rawMessageBatch.groupsCount
    override val sessionAlias: String = rawMessageBatch.sessionAlias()
    override val protocol: String = rawMessageBatch.protocol()
    override val stream: String = rawMessageBatch.stream()

    companion object {
        fun MessageGroupBatch.firstSequence(): Long = requireNotNull(groupsList.first()?.messagesList?.first()?.rawMessage?.sequence) {
            "Message batch should contain one message at least to get first sequnce, batch ${toJson()}"
        }
        fun MessageGroupBatch.lastSequence(): Long = requireNotNull(groupsList.last()?.messagesList?.last()?.rawMessage?.sequence) {
            "Message batch should contain one message at least to get last sequnce, batch ${toJson()}"
        }
        fun MessageGroupBatch.sessionAlias(): String = requireNotNull(groupsList?.first()?.messagesList?.first()?.sessionAlias) {
            "Message batch should contain one message at least to get session alias, batch ${toJson()}"
        }
        fun MessageGroupBatch.protocol(): String = requireNotNull(groupsList?.first()?.messagesList?.first()?.let {

            when {
                it.hasRawMessage() -> it.rawMessage.metadata.protocol
                it.hasMessage() -> it.message.metadata.protocol
                else -> error("Unsupported message kind '${it.kindCase}'")
            }
        }) {
            "Message batch should contain one message at least to get protocol, batch ${toJson()}"
        }
        fun MessageGroupBatch.stream(): String = "${sessionAlias()}:${groupsList.first()?.messagesList?.first()?.rawMessage?.direction.toString()}"

        fun fromMessageGroupBatch(groupBatch: MessageGroupBatch): CodecRequestId {
            return CodecRequestId(
                groupBatch.groupsList
                    .flatMap { group ->
                        group.messagesList.map { anyMessage ->
                            when {
                                anyMessage.hasMessage() -> CodecRequestId.BaseMessageId(anyMessage.message.metadata.id)
                                anyMessage.hasRawMessage() -> CodecRequestId.BaseMessageId(anyMessage.rawMessage.metadata.id)
                                else -> error("Unsupported message kind '${anyMessage.kindCase}'")
                            }
                        }
                    }
                    .toSet()
            )
        }
    }
}

class TransportCodecBatchRequest(
    override val rawMessageBatch: GroupBatch,
    streamName: String
): CodecBatchRequest<GroupBatch, TransportMessageGroup, ParsedMessage>(streamName) {
    override val requestId = fromMessageGroupBatch(rawMessageBatch)
    override val firstSequence: Long = rawMessageBatch.firstSequence()
    override val lastSequence: Long = rawMessageBatch.lastSequence()
    override val groupsCount: Int = rawMessageBatch.groups.size
    override val sessionAlias: String = rawMessageBatch.sessionAlias()
    override val protocol: String = rawMessageBatch.protocol()
    override val stream: String = rawMessageBatch.stream()

    companion object {
        fun GroupBatch.firstSequence(): Long = requireNotNull(groups.firstOrNull()?.messages?.firstOrNull()?.id?.sequence) {
            "Message batch should contain one message at least to get first sequnce, batch $this"
        }
        fun GroupBatch.lastSequence(): Long = requireNotNull(groups.lastOrNull()?.messages?.lastOrNull()?.id?.sequence) {
            "Message batch should contain one message at least to get last sequnce, batch $this"
        }
        fun GroupBatch.sessionAlias(): String = requireNotNull(groups.firstOrNull()?.messages?.firstOrNull()?.id?.sessionAlias) {
            "Message batch should contain one message at least to get session alias, batch $this"
        }
        fun GroupBatch.protocol(): String = requireNotNull(groups.firstOrNull()?.messages?.firstOrNull()?.protocol) {
            "Message batch should contain one message at least to get protocol, batch $this"
        }
        fun GroupBatch.stream(): String = "${sessionAlias()}:${groups.first().messages.first().id.direction.proto}"

        fun fromMessageGroupBatch(groupBatch: GroupBatch): CodecRequestId {
            return CodecRequestId(
                groupBatch.groups
                    .flatMap { group ->
                        group.messages.map {
                            CodecRequestId.BaseMessageId(it.id)
                        }
                    }
                    .toSet()
            )
        }
    }
}

abstract class MessageGroupBatchWrapper<G, PM>{
    abstract val requestId: CodecRequestId
    abstract val groups: List<G>
    abstract val groupCount: Int

    val responseHash: Int
        get() = requestId.hashCode()
    val responseTime = System.currentTimeMillis()

    abstract fun findMessage(sequence: Long): List<PM>
}

class ProtoMessageGroupBatchWrapper(
    private val messageGroupBatch: MessageGroupBatch
): MessageGroupBatchWrapper<ProtoMessageGroup, Message>() {
    override val requestId = ProtoCodecBatchRequest.fromMessageGroupBatch(messageGroupBatch)
    override val groups: List<ProtoMessageGroup>
        get() = messageGroupBatch.groupsList
    override val groupCount: Int
        get() = messageGroupBatch.groupsCount

    override fun findMessage(sequence: Long): List<Message> = messageGroupBatch.groupsList.asSequence()
        .flatMap(ProtoMessageGroup::getMessagesList)
        .filter { it.hasMessage() && it.sequence == sequence }
        .map(AnyMessage::getMessage)
        .toList()
}

class TransportMessageGroupBatchWrapper(
    private val messageGroupBatch: GroupBatch
): MessageGroupBatchWrapper<TransportMessageGroup, ParsedMessage>() {
    override val requestId = TransportCodecBatchRequest.fromMessageGroupBatch(messageGroupBatch)
    override val groups: List<MessageGroup>
        get() = messageGroupBatch.groups
    override val groupCount: Int
        get() = messageGroupBatch.groups.size

    override fun findMessage(sequence: Long): List<ParsedMessage> = messageGroupBatch.groups.asSequence()
        .flatMap(TransportMessageGroup::messages)
        .filter { it is ParsedMessage && it.id.sequence == sequence }
        .map { it as ParsedMessage }
        .toList()
}

class  PendingCodecBatchRequest<G, PM>(
    val completableDeferred: CompletableDeferred< MessageGroupBatchWrapper<G, PM>?>,
    val streamName: String
) {
    val startTimestamp = System.currentTimeMillis()

    var timeoutHandler: Job? = null
        set(value) {
            if (field == null) {
                field = value
            } else {
                throw IllegalArgumentException("Timeout handler already set")
            }
        }

    fun complete(message:  MessageGroupBatchWrapper<G, PM>?): Boolean {
        val isComplete = completableDeferred.complete(message)
        timeoutHandler?.cancel()

        if (message != null) {
            val timeNow = System.currentTimeMillis()
            logger.debug { "${message.requestId} mqCallbackScope ${timeNow - startTimestamp}ms" }
        }

        return isComplete
    }

    fun toResponse(): CodecBatchResponse<G, PM> {
        return CodecBatchResponse(completableDeferred)
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}

class CodecBatchResponse<G, PM>(
    val parsedMessageBatch: CompletableDeferred<MessageGroupBatchWrapper<G, PM>?>
)
