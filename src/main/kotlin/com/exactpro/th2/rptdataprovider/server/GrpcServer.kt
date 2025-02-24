/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.server

import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.dataprovider.grpc.MessageData
import com.exactpro.th2.rptdataprovider.Context
import com.exactpro.th2.rptdataprovider.entities.internal.MessageWithMetadata
import com.exactpro.th2.rptdataprovider.grpc.RptDataProviderGrpcHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.utils.io.InternalAPI
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.util.concurrent.TimeUnit


@InternalAPI
@ExperimentalCoroutinesApi
class GrpcServer<B, G, RM, PM>(
    private val context: Context<B, G, RM, PM>,
    grpcRouter: GrpcRouter,
    converterFun: (MessageWithMetadata<RM, PM>) -> List<MessageData>
) {

    private val reportDataProviderServer = RptDataProviderGrpcHandler(context, converterFun)
    private val server = grpcRouter.startServer(reportDataProviderServer)

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }

    fun start() {
        this.server.start()
        LOGGER.info("${GrpcServer::class.simpleName} started. " +
                "Host: '${context.grpcConfig.serverConfiguration.host}' " +
                "port: '${context.grpcConfig.serverConfiguration.port}'")
    }

    fun stop() {
        if (server.shutdown().awaitTermination(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Server isn't stopped gracefully")
            server.shutdownNow()
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    fun blockUntilShutdown() {
        server?.awaitTermination()
    }
}