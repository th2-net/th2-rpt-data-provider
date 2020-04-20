package com.exactpro.th2.reportdataprovider

import java.time.Instant

data class MessageSearchRequest(
    val attachedEventId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val stream: String?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedEventId = parameters["attachedEventId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.toLong()?.let(Instant::ofEpochMilli),
        timestampTo = parameters["timestampTo"]?.first()?.toLong()?.let(Instant::ofEpochMilli),
        stream = parameters["stream"]?.first(),
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: true
    )
}

data class EventSearchRequest(
    val attachedMessageId: String?,
    val timestampFrom: Instant?,
    val timestampTo: Instant?,
    val name: String?,
    val type: String?,
    val parentEventId: String?,
    val isRootEvent: Boolean?,
    val idsOnly: Boolean
) {
    constructor(parameters: Map<String, List<String>>) : this(
        attachedMessageId = parameters["attachedMessageId"]?.first(),
        timestampFrom = parameters["timestampFrom"]?.first()?.toLong()?.let(Instant::ofEpochMilli),
        timestampTo = parameters["timestampTo"]?.first()?.toLong()?.let(Instant::ofEpochMilli),
        name = parameters["name"]?.first(),
        type = parameters["type"]?.first(),
        parentEventId = parameters["parentEventId"]?.first(),
        isRootEvent = parameters["isRootEvent"]?.first()?.toBoolean(),
        idsOnly = parameters["idsOnly"]?.first()?.toBoolean() ?: true
    )
}
