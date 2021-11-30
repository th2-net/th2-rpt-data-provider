/*******************************************************************************
 * Copyright (c) 2022-2022, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import java.time.Instant


data class StreamName(
    val name: String,
    val direction: Direction,
    val bookId: BookId
) {
    private val fullName = "$name:$direction"

    override fun toString(): String {
        return fullName
    }
}


data class StreamPointer(
    val stream: StreamName,
    val sequence: Long,
    val timestamp: Instant,
    val hasStarted: Boolean
) {
    constructor(
        sequence: Long,
        streamName: String,
        direction: Direction,
        bookId: BookId,
        timestamp: Instant,
        hasStarted: Boolean
    ) : this(
        stream = StreamName(streamName, direction, bookId),
        sequence = sequence,
        timestamp = timestamp,
        hasStarted = hasStarted
    )
}

