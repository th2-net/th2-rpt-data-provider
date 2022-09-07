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

import com.exactpro.cradle.testevents.StoredTestEventWithContent
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity

data class IntermediateEvent(
    val cradleEvent: StoredTestEventWithContent,
    val baseEventEntity: BaseEventEntity
)
