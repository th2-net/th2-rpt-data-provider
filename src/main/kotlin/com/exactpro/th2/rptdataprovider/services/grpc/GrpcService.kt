/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.services.grpc

import com.exactpro.th2.codec.grpc.AsyncCodecService
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.metrics.SESSION_ALIAS_LABEL
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.exactpro.th2.rptdataprovider.services.AbstractDecoderService
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class GrpcService(
    configuration: Configuration,
    grpcRouter: GrpcRouter
) : AbstractDecoderService(configuration) {

    private val codecService = grpcRouter.getService(AsyncCodecService::class.java)
    private val responseObserver = object : StreamObserver<MessageGroupBatch> {
        override fun onNext(decodedBatch: MessageGroupBatch) = handleResponse(decodedBatch)
        override fun onError(t: Throwable?) = LOGGER.error(t) { "gRPC request failed." }
        override fun onCompleted() {}
    }

    override fun decode(messageGroupBatch: MessageGroupBatch) = codecService.decode(messageGroupBatch, responseObserver)
    override fun decode(messageGroupBatch: MessageGroupBatch, sessionAlias: String) =
        codecService.decode(messageGroupBatch, mapOf(SESSION_ALIAS_LABEL to sessionAlias), responseObserver)

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}