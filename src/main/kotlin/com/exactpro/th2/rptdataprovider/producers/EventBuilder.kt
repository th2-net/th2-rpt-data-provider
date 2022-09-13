/*******************************************************************************
 * Copyright (c) 2022-2022, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.producers

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.event.EventUtils.generateUUID
import com.exactpro.th2.common.event.EventUtils.toEventID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.rptdataprovider.convertToProto
import com.google.protobuf.ByteString
import java.time.Instant

class EventBuilder {
    private var name: String? = null
    private var type: String? = null
    private var status: EventStatus = EventStatus.SUCCESS
    private var data: ByteArray? = null
    private var parentEventID: EventID? = null
    private var attachedIds: List<StoredMessageId> = emptyList()


    fun name(name: String) = apply { this.name = name }
    fun type(type: String) = apply { this.type = type }
    fun status(status: EventStatus) = apply { this.status = status }
    fun data(data: ByteArray) = apply { this.data = data }
    fun parentId(eventID: EventID) = apply { this.parentEventID = eventID }
    fun attachedIds(attachedIds: List<StoredMessageId>) = apply { this.attachedIds = attachedIds }

    fun build(): com.exactpro.th2.common.grpc.Event {
        return com.exactpro.th2.common.grpc.Event.newBuilder()
            .setStartTimestamp(Instant.now().toTimestamp())
            .setEndTimestamp(Instant.now().toTimestamp())
            .setId(toEventID(generateUUID()))
            .setName(name)
            .setStatus(status)
            .addAllAttachedMessageIds(attachedIds.map { it.convertToProto() })
            .also { builder ->
                type?.let { builder.setType(it) }
                parentEventID?.let { builder.setParentId(it) }
                data?.let { builder.setBody(ByteString.copyFrom(it)) }
            }
            .build()
    }
}