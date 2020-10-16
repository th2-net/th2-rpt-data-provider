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

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.cradle.testevents.StoredTestEventId

class ProviderEventId(val batchId: StoredTestEventId?, val eventId: StoredTestEventId) {
    companion object {
        const val divider = ":"
    }

    constructor(id: String) : this(
        batchId = id.split(divider).getOrNull(0)?.let { StoredTestEventId(it) }?.takeIf { id.contains(
            divider
        ) },
        eventId = id.split(divider).getOrNull(1)?.let { StoredTestEventId(it) } ?: StoredTestEventId(id)
    )

    override fun toString(): String {
        return (batchId?.toString()?.let { it + divider } ?: "") + eventId.toString()
    }


}
