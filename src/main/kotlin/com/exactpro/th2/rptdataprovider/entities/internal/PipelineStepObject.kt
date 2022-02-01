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

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchResponse
import java.time.Instant

data class StreamEndObject(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant
) : PipelineStepObject


interface PipelineStepObject {
    val streamEmpty: Boolean
    val lastProcessedId: StoredMessageId?
    val lastScannedTime: Instant
}


data class EmptyPipelineObject(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject) : this(
        pipelineStepObject.streamEmpty, pipelineStepObject.lastProcessedId, pipelineStepObject.lastScannedTime
    )
}


data class PipelineKeepAlive(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val scannedObjectsCount: Long
) : PipelineStepObject {

    constructor(pipelineStepObject: PipelineStepObject, scannedObjectsCount: Long) : this(
        pipelineStepObject.streamEmpty,
        pipelineStepObject.lastProcessedId,
        pipelineStepObject.lastScannedTime,
        scannedObjectsCount
    )
}


data class PipelineRawBatch(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val storedBatchWrapper: MessageBatchWrapper
) : PipelineStepObject


data class PipelineCodecRequest(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val storedBatchWrapper: MessageBatchWrapper,
    val codecRequest: CodecBatchRequest
): PipelineStepObject


data class PipelineDecodedBatch(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val storedBatchWrapper: MessageBatchWrapper,
    val codecResponse: CodecBatchResponse
): PipelineStepObject


data class PipelineParsedMessage(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val payload: Message
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject, payload: Message) : this(
        pipelineStepObject.streamEmpty, payload.id, payload.timestamp, payload
    )
}


data class PipelineFilteredMessage(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val payload: MessageWithMetadata
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject, payload: MessageWithMetadata) : this(
        pipelineStepObject.streamEmpty, pipelineStepObject.lastProcessedId, pipelineStepObject.lastScannedTime, payload
    )
}
