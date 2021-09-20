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

import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.responses.MessageBatchWrapper
import com.exactpro.th2.rptdataprovider.entities.responses.ParsedMessageBatch
import java.time.Instant

interface MessagePipelineStepObject {
    val streamEmpty: Boolean
    val payload: Any?
    val lastProcessedId: StoredMessageId?
    val lastScannedTime: Instant
}

data class RawBatch(
    override val streamEmpty: Boolean,
    override val payload: MessageBatchWrapper?,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant,
    val needReverseBatch: Boolean
) : MessagePipelineStepObject {
    constructor(
        streamEmpty: Boolean,
        payload: MessageBatchWrapper?,
        lastProcessedId: StoredMessageId?,
        lastScannedTime: Instant,
        direction: TimeRelation
    ) : this(streamEmpty, payload, lastProcessedId, lastScannedTime, direction == TimeRelation.BEFORE)
}


data class ParsedMessage(
    override val streamEmpty: Boolean,
    override val payload: Message?,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant
) : MessagePipelineStepObject


data class FilterWrappedParsedMessage(
    override val streamEmpty: Boolean,
    override val payload: MessageWithMetadata?,
    override val lastProcessedId: StoredMessageId?,
    override val lastScannedTime: Instant
) : MessagePipelineStepObject
