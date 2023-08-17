/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.entities.configuration.CustomConfigurationClass
import com.exactpro.th2.rptdataprovider.entities.mappers.ProtoMessageMapper
import com.exactpro.th2.rptdataprovider.entities.mappers.TransportMessageMapper
import com.exactpro.th2.rptdataprovider.server.GrpcServer
import com.exactpro.th2.rptdataprovider.server.HttpServer
import com.exactpro.th2.rptdataprovider.server.ServerType
import com.exactpro.th2.rptdataprovider.server.ServerType.GRPC
import com.exactpro.th2.rptdataprovider.server.ServerType.HTTP
import io.ktor.server.engine.*
import io.ktor.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.IO_PARALLELISM_PROPERTY_NAME
import kotlinx.coroutines.InternalCoroutinesApi
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class Main @InternalAPI constructor(args: Array<String>) {

    private val commonFactory: CommonFactory
    private val configuration: Configuration

    private val protoContext: ProtoContext?
    private val transportContext: TransportContext?

    private val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()

    init {
        configureShutdownHook(resources, lock, condition)
        commonFactory = CommonFactory.createFromArguments(*args)
        resources += commonFactory
        configuration =
            Configuration(commonFactory.getCustomConfiguration(CustomConfigurationClass::class.java))
        if (configuration.useTransportMode.value.toBoolean()) {
            protoContext = null
            transportContext = TransportContext(
                configuration,

                serverType = ServerType.valueOf(configuration.serverType.value),

                cradleManager = commonFactory.cradleManager.also {
                    resources += it
                },
                transportMessageRouterPublisher = commonFactory.transportGroupBatchRouter.also {
                    resources += it
                },
                transportMessageRouterSubscriber = commonFactory.transportGroupBatchRouter.also {
                    resources += it
                },
                grpcConfig = commonFactory.grpcConfiguration
            )
        } else {
            protoContext = ProtoContext(
                configuration,

                serverType = ServerType.valueOf(configuration.serverType.value),

                cradleManager = commonFactory.cradleManager.also {
                    resources += it
                },
                protoMessageRouterPublisher = commonFactory.messageRouterMessageGroupBatch.also {
                    resources += it
                },
                protoMessageRouterSubscriber = commonFactory.messageRouterMessageGroupBatch.also {
                    resources += it
                },
                grpcConfig = commonFactory.grpcConfiguration
            )
            transportContext = null
        }
    }


    @InternalCoroutinesApi
    @ExperimentalCoroutinesApi
    @FlowPreview
    @InternalAPI
    fun run() {
        logger.info { "Starting the box" }

        LIVENESS_MONITOR.enable()

        startServer()

        READINESS_MONITOR.enable()

        awaitShutdown(lock, condition)
    }

    @InternalCoroutinesApi
    @ExperimentalCoroutinesApi
    @FlowPreview
    @InternalAPI
    private fun startServer() {

        if (configuration.useTransportMode.value.toBoolean()) {
            requireNotNull(transportContext) {
                "Transport context can't be null"
            }

            System.setProperty(
                IO_PARALLELISM_PROPERTY_NAME,
                transportContext.configuration.ioDispatcherThreadPoolSize.value
            )

            when (transportContext.serverType) {
                HTTP -> {
                    HttpServer(transportContext, TransportMessageMapper::convertToHttpMessage).run()
                }
                GRPC -> {
                    val grpcRouter = commonFactory.grpcRouter
                    resources += grpcRouter

                    val grpcServer = GrpcServer(transportContext, grpcRouter, TransportMessageMapper::convertToTransportMessageData)

                    resources += AutoCloseable { grpcServer.stop() }
                    grpcServer.start()
                }
            }
        } else {
            requireNotNull(protoContext) {
                "Protobuf context can't be null"
            }

            System.setProperty(
                IO_PARALLELISM_PROPERTY_NAME,
                protoContext.configuration.ioDispatcherThreadPoolSize.value
            )

            when (protoContext.serverType) {
                HTTP -> {
                    HttpServer(protoContext, ProtoMessageMapper::convertToHttpMessage).run()
                }

                GRPC -> {
                    val grpcRouter = commonFactory.grpcRouter
                    resources += grpcRouter

                    val grpcServer = GrpcServer(protoContext, grpcRouter, ProtoMessageMapper::convertToGrpcMessageData)

                    resources += AutoCloseable { grpcServer.stop() }
                    grpcServer.start()
                }
            }
        }
    }

    private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
        Runtime.getRuntime().addShutdownHook(thread(
            start = false,
            name = "Shutdown hook"
        ) {
            logger.info { "Shutdown start" }
            READINESS_MONITOR.disable()
            try {
                lock.lock()
                condition.signalAll()
            } finally {
                lock.unlock()
            }
            resources.descendingIterator().forEachRemaining { resource ->
                try {
                    resource.close()
                } catch (e: Exception) {
                    logger.error(e) { "cannot close resource ${resource::class}" }
                }
            }
            LIVENESS_MONITOR.disable()
            logger.info { "Shutdown end" }
        })
    }

    @Throws(InterruptedException::class)
    private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
        try {
            lock.lock()
            logger.info { "Wait shutdown" }
            condition.await()
            logger.info { "App shutdown" }
        } finally {
            lock.unlock()
        }
    }
}


@InternalCoroutinesApi
@FlowPreview
@InternalAPI
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    try {
        Main(args).run()
    } catch (ex: Exception) {
        logger.error(ex) { "cannot start the box" }
        exitProcess(1)
    }
}
