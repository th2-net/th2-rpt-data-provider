/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.responses

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.Stream
import com.exactpro.th2.rptdataprovider.cradleDirectionToGrpc
import com.exactpro.th2.rptdataprovider.convertToProto
import java.time.Instant

data class StreamInfo(val stream: Pair<String, Direction>, val keepOpen: Boolean, val startTimestamp: Instant) {
    var lastElement: StoredMessageId? = null
        private set
    var lastElementTime: Instant? = null
        private set
    var isFirstPull: Boolean = true
        private set


    constructor(
        stream: Pair<String, Direction>,
        startId: StoredMessageId?,
        time: Instant?,
        keepOpen: Boolean,
        startTimestamp: Instant,
        firstPull: Boolean = true
    ) : this(
        stream = stream,
        keepOpen = keepOpen,
        startTimestamp = startTimestamp
    ) {
        lastElement = startId
        lastElementTime = time
        isFirstPull = firstPull
    }

    fun update(
        size: Int,
        perStreamLimit: Int,
        timelineDirection: TimeRelation,
        filteredIdsList: List<MessageWrapper>
    ) {
        val streamIsEmpty = size < perStreamLimit
        changeStreamMessageIndex(streamIsEmpty, filteredIdsList, timelineDirection).let {
            lastElement = it.first
            lastElementTime = it.second
        }
        isFirstPull = false
    }


    private fun changeStreamMessageIndex(
        streamIsEmpty: Boolean,
        filteredIdsList: List<MessageWrapper>,
        timelineDirection: TimeRelation
    ): Pair<StoredMessageId?, Instant?> {
        return if (timelineDirection == TimeRelation.AFTER) {
            filteredIdsList.lastOrNull()?.message?.let { it.id to it.timestamp }
                ?: if (keepOpen) lastElement to lastElementTime else null to null
        } else {
            if (!streamIsEmpty) {
                val lastMessage = filteredIdsList.firstOrNull()?.message
                lastMessage?.id to lastMessage?.timestamp
            } else {
                null to null
            }
        }
    }

    fun convertToProto(): Stream {
        return Stream.newBuilder()
            .setDirection(cradleDirectionToGrpc(stream.second))
            .setSession(stream.first).also { builder ->
                lastElement?.let { builder.setLastId(it.convertToProto()) }
            }.build()
    }
}