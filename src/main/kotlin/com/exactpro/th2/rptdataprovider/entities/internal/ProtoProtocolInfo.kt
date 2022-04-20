package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.rptdataprovider.handlers.messages.MessageBatchDecoder
import mu.KotlinLogging

object ProtoProtocolInfo {
    val logger = KotlinLogging.logger{}

    private val messageProtocolDescriptor =
        RawMessage.getDescriptor()
            .findFieldByName("metadata")
            .messageType
            .findFieldByName("protocol")

    private val TYPE_IMAGE = "image"

    fun getProtocolField(rawMessageBatch: MessageGroupBatch): String? {
        val parsedRawMessage = rawMessageBatch.groupsList.first().messagesList.first()
        return try {
            parsedRawMessage.rawMessage.metadata.getField(messageProtocolDescriptor).toString()
        } catch (e: Exception) {
            logger.error(e) { "Field: '${messageProtocolDescriptor.name}' does not exist in message: $parsedRawMessage " }
            null
        }
    }

    fun isImage(protocolName: String?): Boolean {
        return protocolName?.contains(TYPE_IMAGE) ?: false
    }
}