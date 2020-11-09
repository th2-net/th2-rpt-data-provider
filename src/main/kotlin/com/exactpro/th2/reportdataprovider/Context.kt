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

package com.exactpro.th2.reportdataprovider

import com.exactpro.th2.reportdataprovider.cache.CodecCache
import com.exactpro.th2.reportdataprovider.cache.EventCache
import com.exactpro.th2.reportdataprovider.cache.MessageCache
import com.exactpro.th2.reportdataprovider.entities.configuration.Configuration
import com.exactpro.th2.reportdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.reportdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.reportdataprovider.producers.EventProducer
import com.exactpro.th2.reportdataprovider.producers.MessageProducer
import com.exactpro.th2.reportdataprovider.services.cradle.CradleService
import com.exactpro.th2.reportdataprovider.services.rabbitmq.RabbitMqService
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.http.CacheControl

@Suppress("MemberVisibilityCanBePrivate")
class Context(
    val configuration: Configuration = Configuration(),

    val timeout: Long = configuration.responseTimeout.value.toLong(),
    val cacheTimeout: Long = configuration.serverCacheTimeout.value.toLong(),

    val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),

    val cradleService: CradleService = CradleService(configuration),

    val rabbitMqService: RabbitMqService = RabbitMqService(
        configuration
    ),

    val eventProducer: EventProducer = EventProducer(cradleService, jacksonMapper),
    val eventCache: EventCache = EventCache(cacheTimeout, configuration.eventCacheSize.value.toLong(), eventProducer),
    val searchEventsHandler: SearchEventsHandler = SearchEventsHandler(cradleService, timeout),

    val codecCache: CodecCache = CodecCache(configuration),

    val messageProducer: MessageProducer = MessageProducer(cradleService, rabbitMqService, codecCache),
    val messageCache: MessageCache = MessageCache(configuration, messageProducer),
    val searchMessagesHandler: SearchMessagesHandler = SearchMessagesHandler(
        cradleService,
        messageCache,
        messageProducer,
        timeout
    ),

    val cacheControl: CacheControl.MaxAge = configuration.clientCacheTimeout.value.toInt().let {
        CacheControl.MaxAge(
            visibility = CacheControl.Visibility.Public,
            maxAgeSeconds = it,
            mustRevalidate = false,
            proxyRevalidate = false,
            proxyMaxAgeSeconds = it
        )
    }
)
