/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider


import com.exactpro.cradle.CradleManager
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.rptdataprovider.cache.EventCache
import com.exactpro.th2.rptdataprovider.cache.MessageCache
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.filters.PredicateFactory
import com.exactpro.th2.rptdataprovider.entities.filters.events.AttachedMessageFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventBodyFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventNameFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventStatusFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.EventTypeFilter
import com.exactpro.th2.rptdataprovider.entities.filters.events.ParentEventIdFilter
import com.exactpro.th2.rptdataprovider.entities.filters.messages.AttachedEventFilters
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageBodyBinaryFilter
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageBodyFilter
import com.exactpro.th2.rptdataprovider.entities.filters.messages.MessageTypeFilter
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.rptdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.rptdataprovider.handlers.TransportSearchMessagesHandler
import com.exactpro.th2.rptdataprovider.producers.EventProducer
import com.exactpro.th2.rptdataprovider.producers.MessageProducer
import com.exactpro.th2.rptdataprovider.producers.TransportMessageProducer
import com.exactpro.th2.rptdataprovider.server.ServerType
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.RabbitMqService
import com.exactpro.th2.rptdataprovider.services.rabbitmq.TransportRabbitMqService
import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.http.CacheControl

@Suppress("MemberVisibilityCanBePrivate")
class TransportContext(
    configuration: Configuration,

    serverType: ServerType,

    timeout: Long = configuration.responseTimeout.value.toLong(),
    cacheTimeout: Long = configuration.serverCacheTimeout.value.toLong(),

    jacksonMapper: ObjectMapper = JACKSON_MAPPER,

    cradleManager: CradleManager,

    transportMessageRouterPublisher: MessageRouter<GroupBatch>,
    transportMessageRouterSubscriber: MessageRouter<GroupBatch>,

    grpcConfig: GrpcConfiguration,

    cradleService: CradleService = CradleService(
        configuration,
        cradleManager
    ),

    rabbitMqService: RabbitMqService<GroupBatch, MessageGroup, ParsedMessage> = TransportRabbitMqService(
        configuration,
        transportMessageRouterPublisher,
        transportMessageRouterSubscriber
    ),

    eventProducer: EventProducer = EventProducer(cradleService, jacksonMapper),

    eventCache: EventCache = EventCache(cacheTimeout, configuration.eventCacheSize.value.toLong(), eventProducer),

    messageProducer: MessageProducer<RawMessage, ParsedMessage> = TransportMessageProducer(
        cradleService,
        rabbitMqService
    ),

    messageCache: MessageCache<RawMessage, ParsedMessage> = MessageCache(configuration, messageProducer),

    eventFiltersPredicateFactory: PredicateFactory<BaseEventEntity> = PredicateFactory(
        mapOf(
            AttachedMessageFilter.filterInfo to AttachedMessageFilter.Companion::build,
            ParentEventIdFilter.filterInfo to ParentEventIdFilter.Companion::build,
            EventTypeFilter.filterInfo to EventTypeFilter.Companion::build,
            EventNameFilter.filterInfo to EventNameFilter.Companion::build,
            EventBodyFilter.filterInfo to EventBodyFilter.Companion::build,
            EventStatusFilter.filterInfo to EventStatusFilter.Companion::build
        ), cradleService
    ),

    messageFiltersPredicateFactory: PredicateFactory<MessageWithMetadata<RawMessage, ParsedMessage>> = PredicateFactory(
        mapOf(
            AttachedEventFilters.filterInfo to AttachedEventFilters.Companion::build,
            MessageTypeFilter.filterInfo to MessageTypeFilter.Companion::build,
            MessageBodyFilter.filterInfo to MessageBodyFilter.Companion::build,
            MessageBodyBinaryFilter.filterInfo to MessageBodyBinaryFilter.Companion::build
        ), cradleService
    ),


    enableCaching: Boolean = configuration.enableCaching.value.toBoolean(),

    keepAliveTimeout: Long = configuration.keepAliveTimeout.value.toLong(),

    cacheControlNotModified: CacheControl = configuration.notModifiedObjectsLifetime.value.toInt().let {
        cacheControlConfig(it, enableCaching)
    },

    cacheControlRarelyModified: CacheControl = configuration.rarelyModifiedObjects.value.toInt().let {
        cacheControlConfig(it, enableCaching)
    }
): Context<GroupBatch, MessageGroup, RawMessage, ParsedMessage>(
    configuration,
    serverType,
    timeout,
    cacheTimeout,
    jacksonMapper,
    cradleManager,
    grpcConfig,
    cradleService,
    rabbitMqService,
    eventProducer,
    eventCache,
    messageProducer,
    messageCache,
    eventFiltersPredicateFactory,
    messageFiltersPredicateFactory,
    enableCaching,
    keepAliveTimeout,
    cacheControlNotModified,
    cacheControlRarelyModified
) {
    override val searchMessagesHandler: SearchMessagesHandler<GroupBatch, MessageGroup, RawMessage, ParsedMessage> =
        TransportSearchMessagesHandler(this)

}
