/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.services.cradle

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.resultset.CradleResultSet
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.configuration.CustomConfigurationClass
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

class CradleServiceTest {
    @Test
    fun `retrieve empty stream`() {
        val configuration = Configuration(CustomConfigurationClass())
        val cradleManager = mockk<CradleManager>()
        val cradleStorage = mockk<CradleStorage>()
        val resultSet = mockk<CradleResultSet<StoredGroupedMessageBatch>>()

        every { resultSet.hasNext() } answers { false }
        every { cradleManager.storage } answers { cradleStorage }
        every { cradleStorage.getGroupedMessageBatches(any()) } answers { resultSet }

        val cradleService = CradleService(configuration, cradleManager)

        val coroutineScope = CoroutineScope(Dispatchers.Default)

        runBlocking {
            val channel = cradleService.getGroupedMessages(coroutineScope, GroupedMessageFilter(null, null))
            var itemCounter = 0

            withTimeout(50) {
                for (item in channel) {
                    itemCounter++
                }
            }

            assertEquals(0, itemCounter)
        }
    }
}