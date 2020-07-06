package com.exactpro.th2.reportdataprovider.entities

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.reportdataprovider.jacksonMapper
import com.fasterxml.jackson.annotation.JsonRawValue
import mu.KotlinLogging
import java.time.Instant
import java.util.*

data class Event(
    val isBatched: Boolean,
    val type: String = "event",
    val eventId: String,
    val eventName: String,
    val eventType: String?,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: String?,
    val isSuccessful: Boolean,
    val attachedMessageIds: Set<String>?,
    val childrenIds: List<String>,

    @JsonRawValue
    val body: String?
) {
    constructor(
        stored: StoredTestEventWithContent,
        cradleManager: CradleManager,
        childrenIds: List<String>,
        isBatched: Boolean
    ) : this(
        childrenIds = childrenIds,
        isBatched = isBatched,
        eventId = stored.id.toString(),
        eventName = stored.name ?: "unknown",
        eventType = stored.type ?: "unknown",
        startTimestamp = stored.startTimestamp,
        endTimestamp = stored.endTimestamp,
        parentEventId = stored.parentId?.toString(),
        isSuccessful = stored.isSuccess,

        attachedMessageIds = stored.id.let {
            try {
                cradleManager.storage?.testEventsMessagesLinker
                    ?.getMessageIdsByTestEventId(it)?.map(Any::toString)?.toSet().orEmpty()
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error(e) { "unable to get messages attached to event (id=${stored.id})" }

                Collections.emptySet<String>()
            }
        },

        body = stored.content.let {
            try {
                val data = String(it).takeUnless(String::isEmpty) ?: "{}"
                jacksonMapper.readTree(data)
                data
            } catch (e: Exception) {
                KotlinLogging.logger { }
                    .error(e) { "unable to write event content (id=${stored.id}) to 'body' property - invalid data" }

                null
            }
        }
    )
}
