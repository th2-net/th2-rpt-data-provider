/*******************************************************************************
 * Copyright (c) 2022-2022, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.th2.rptdataprovider.handlers.events

import com.exactpro.th2.rptdataprovider.entities.internal.ProviderEventId
import java.time.Instant

data class SearchInterval(
    val startInterval: Instant,
    val endInterval: Instant,
    val startRequest: Instant,
    val endRequest: Instant?,
    private val providerResumeId: ProviderEventId? = null
) {
    val resumeId = providerResumeId?.let { it.batchId ?: it.eventId }
}