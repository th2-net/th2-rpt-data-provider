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
import com.exactpro.th2.rptdataprovider.entities.responses.StoredMessageBatchWrapper
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchRequest
import com.exactpro.th2.rptdataprovider.services.rabbitmq.CodecBatchResponse
import java.time.Instant
import kotlin.math.max

class PipelineStepsInfo {
    var startExtract: Long = 0
    var endExtract: Long = 0
    var startConvert: Long = 0
    var endConvert: Long = 0
    var startParseMessage: Long = 0
    var endParseMessage: Long = 0
    var codecResponse: Long = 0
    var startFilter: Long = 0
    var endFilter: Long = 0

    var buildMessage: Long = 0
    var serializingTime: Long = 0

    fun extractTime() = endExtract - startExtract

    fun convertTime() = endConvert - startConvert

    fun decodeCodecResponse() = max(codecResponse - startParseMessage, 0)

    fun decodeTimeAll() = endParseMessage - startParseMessage

    fun filterTime() = endFilter - startFilter
}


interface PipelineStepObject {
    val streamEmpty: Boolean
    val lastProcessedId: StoredMessageId?
    val lastScannedTime: Instant
    val info: PipelineStepsInfo
}

data class StreamEndObject(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    override val info: PipelineStepsInfo = PipelineStepsInfo()
) : PipelineStepObject


// FIXME: restore "data class" declaration
class EmptyPipelineObject(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    override val info: PipelineStepsInfo = PipelineStepsInfo()
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject) : this(
        pipelineStepObject.streamEmpty,
        pipelineStepObject.lastProcessedId,
        pipelineStepObject.lastScannedTime
    )
}


data class PipelineKeepAlive(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val scannedObjectsCount: Long,
    override val info: PipelineStepsInfo = PipelineStepsInfo()
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
    val storedBatchWrapper: StoredMessageBatchWrapper,
    override val info: PipelineStepsInfo = PipelineStepsInfo()
) : PipelineStepObject


data class PipelineCodecRequest(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val storedBatchWrapper: MessageBatchWrapper,
    val codecRequest: CodecBatchRequest,
    override val info: PipelineStepsInfo
) : PipelineStepObject


data class PipelineDecodedBatch(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val storedBatchWrapper: MessageBatchWrapper,
    override val info: PipelineStepsInfo,
    val codecResponse: CodecBatchResponse,
    val imageType: String?
) : PipelineStepObject {
    constructor(
        pipelineMessage: PipelineCodecRequest,
        codecBatchResponse: CodecBatchResponse,
        imageType: String? = null
    ) : this(
        pipelineMessage.streamEmpty,
        pipelineMessage.lastProcessedId,
        pipelineMessage.lastScannedTime,
        pipelineMessage.storedBatchWrapper,
        pipelineMessage.info,
        codecBatchResponse,
        imageType
    )
}


data class PipelineParsedMessage(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    override val info: PipelineStepsInfo,
    val payload: Message
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject, payload: Message) : this(
        pipelineStepObject.streamEmpty,
        payload.id,
        payload.timestamp,
        pipelineStepObject.info,
        payload
    )
}


data class PipelineFilteredMessage(
    override val streamEmpty: Boolean,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    override val info: PipelineStepsInfo,
    val payload: MessageWithMetadata
) : PipelineStepObject {
    constructor(pipelineStepObject: PipelineStepObject, payload: MessageWithMetadata) : this(
        pipelineStepObject.streamEmpty,
        pipelineStepObject.lastProcessedId,
        pipelineStepObject.lastScannedTime,
        pipelineStepObject.info,
        payload
    )
}
