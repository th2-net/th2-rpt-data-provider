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
package com.exactpro.th2.rptdataprovider.handlers.messages

import com.exactpro.th2.rptdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.rptdataprovider.entities.sse.StreamWriter
import com.exactpro.th2.rptdataprovider.handlers.SearchMessagesHandler

class SearchMessagesCalledFun<B, G, RM, PM>(
    private val searchMessagesHandler: SearchMessagesHandler<B, G, RM, PM>,
    private val request: SseMessageSearchRequest<RM, PM>,
) {
    suspend fun calledFun(streamWriter: StreamWriter<RM, PM>) {
        request.checkRequest()
        searchMessagesHandler.searchMessagesSse(request, streamWriter)
    }
}