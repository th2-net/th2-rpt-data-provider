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

import com.exactpro.th2.rptdataprovider.server.GrpcServer
import com.exactpro.th2.rptdataprovider.server.HttpServer
import io.ktor.server.engine.*
import io.ktor.util.*

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.lang.IllegalArgumentException


@FlowPreview
@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    val context = Context(args)
    when (val type = context.configuration.serverType.value) {
        "http" -> {
            GlobalScope.launch {
                HttpServer(context).run()
            }
        }
        "grpc" -> {
            GlobalScope.launch {
                GrpcServer(context)
            }
        }
        else -> throw IllegalArgumentException("Unsupported server type: $type. Valid types is 'http' or 'grpc'")
    }
}
