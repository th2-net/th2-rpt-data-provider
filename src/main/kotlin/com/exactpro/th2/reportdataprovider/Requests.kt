package com.exactpro.th2.reportdataprovider

import java.time.Instant

data class MessageSearchRequest(
    val attachedEventId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val stream: List<String>?,
    val messageType: List<String>?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedEventId = parameters["attachedEventId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        timestampTo = parameters["timestampTo"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"],
        messageType = parameters["messageType"],
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: true
    )
}

data class EventSearchRequest(
    val attachedMessageId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val name: String?,
    val type: String?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedMessageId = parameters["attachedMessageId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        timestampTo = parameters["timestampTo"]?.first()?.let { Instant.ofEpochMilli(it.toLong()) },
        name = parameters["name"]?.first(),
        type = parameters["type"]?.first(),
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: false
    )
}
