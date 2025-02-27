/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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
 */
package com.exactpro.th2.rptdataprovider.handlers.events

import com.exactpro.th2.rptdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.SearchEventsHandler
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview

class SearchEventsCalledFun<RM, PM>(
    private val searchEventsHandler: SearchEventsHandler,
    val request: SseEventSearchRequest
) {
    @OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
    suspend fun calledFun(streamWriter: StreamWriter<RM, PM>) {
        request.checkRequest()
        searchEventsHandler.searchEventsSse(request, streamWriter)
    }
}