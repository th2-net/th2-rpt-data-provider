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

package com.exactpro.th2.rptdataprovider


import com.exactpro.th2.rptdataprovider.cache.CodecCache
import com.exactpro.th2.rptdataprovider.cache.EventCache
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.filters.PredicateFactory
import com.exactpro.th2.rptdataprovider.entities.filters.events.AttachedMessageFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventNameFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventTypeFilter
import com.exactpro.th2.rptdataprovider.entities.filters.messages.AttachedEventFilters
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageBodyFilter
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageTypeFilter
import com.exactpro.th2.rptdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.rptdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.http.*

@Suppress("MemberVisibilityCanBePrivate")
class Context(
    val args: Array<String>,
    val configuration: Configuration = Configuration(args),

    val timeout: Long = configuration.responseTimeout.value.toLong(),
    val cacheTimeout: Long = configuration.serverCacheTimeout.value.toLong(),

    val sseEventSearchStep: Long = configuration.sseEventSearchStep.value.toLong(),

    val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),

    val cradleService: CradleService = CradleService(
        configuration
    ),

    val rabbitMqService: RabbitMqService = RabbitMqService(
        configuration
    ),

    val eventProducer: EventProducer = EventProducer(cradleService, jacksonMapper),
    val eventCache: EventCache = EventCache(cacheTimeout, configuration.eventCacheSize.value.toLong(), eventProducer),
    val searchEventsHandler: SearchEventsHandler = SearchEventsHandler(
        cradleService,
        configuration.dbRetryDelay.value.toLong()
    ),

    val codecCache: CodecCache = CodecCache(configuration),

    val messageProducer: MessageProducer = MessageProducer(cradleService, rabbitMqService, codecCache),
    val messageCache: MessageCache = MessageCache(configuration, messageProducer),
    val searchMessagesHandler: SearchMessagesHandler = SearchMessagesHandler(
        cradleService,
        messageCache,
        configuration.maxMessagesLimit.value.toInt(),
        configuration.messageSearchPipelineBuffer.value.toInt(),
        configuration.dbRetryDelay.value.toLong()
    ),

    val eventFiltersPredicateFactory: PredicateFactory<EventTreeNode> = PredicateFactory(
        mapOf(
            AttachedMessageFilter.filterInfo to ::AttachedMessageFilter,
            EventTypeFilter.filterInfo to ::EventTypeFilter,
            EventNameFilter.filterInfo to ::EventNameFilter
        ), cradleService
    ),

    val messageFiltersPredicateFactory: PredicateFactory<Message> = PredicateFactory(
        mapOf(
            AttachedEventFilters.filterInfo to ::AttachedEventFilters,
            MessageTypeFilter.filterInfo to ::MessageTypeFilter,
            MessageBodyFilter.filterInfo to ::MessageBodyFilter
        ), cradleService
    ),


    private val enableCaching: Boolean = configuration.enableCaching.value.toBoolean(),

    val cacheControlNotModified: CacheControl = configuration.notModifiedObjectsLifetime.value.toInt().let {
        cacheControlConfig(it, enableCaching)
    },

    val cacheControlRarelyModified: CacheControl = configuration.rarelyModifiedObjects.value.toInt().let {
        cacheControlConfig(it, enableCaching)
    },

    val cacheControlFrequentlyModified: CacheControl = configuration.frequentlyModifiedObjects.value.toInt().let {
        cacheControlConfig(it, enableCaching)
    }
)

private fun cacheControlConfig(timeout: Int, enableCaching: Boolean): CacheControl {
    return if (enableCaching) {
        CacheControl.MaxAge(
            visibility = CacheControl.Visibility.Public,
            maxAgeSeconds = timeout,
            mustRevalidate = false,
            proxyRevalidate = false,
            proxyMaxAgeSeconds = timeout
        )
    } else {
        CacheControl.NoCache(
            visibility = CacheControl.Visibility.Public
        )
    }
}
