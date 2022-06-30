/*******************************************************************************
 * Copyright (c) 2022-2022, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.rptdataprovider.convertToProto
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.google.protobuf.ByteString
import mu.KotlinLogging

object ProtoMessageMapper {

    private val logger = KotlinLogging.logger { }

    fun storedMessageToRawProto(storedMessage: StoredMessage): RawMessage {
        return RawMessage.newBuilder()
            .setMetadata(
                RawMessageMetadata.newBuilder()
                    .setId(storedMessage.id.convertToProto())
                    .setTimestamp(storedMessage.timestamp.toTimestamp())
                    .build()
            ).also { builder ->
                storedMessage.content?.let {
                    logger.error { "Received stored message has no content. StoredMessageId: ${storedMessage.id}" }
                    builder.setBody(ByteString.copyFrom(it))
                }
            }
            .build()
    }
}