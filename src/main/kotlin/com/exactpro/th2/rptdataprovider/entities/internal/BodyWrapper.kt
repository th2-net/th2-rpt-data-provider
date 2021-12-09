/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.entities.internal

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.getField


data class BodyWrapper(
    val id: MessageID,
    val protocol: String,
    val messageType: String,
    val message: Message
) {
    constructor(message: Message) : this(
        message.metadata.id,
        message.metadata.protocol,
        message.metadata.messageType,
        message
    )

    /*fun removeFields (body:BodyWrapper)
    {
        var key = body.message.getField("fields")
        //body.message.allFields.remove(key)
        body.message.allFields.map { it.key.toString().toUpperCase() }
        body.message.allFields.map { println("KEY = " + it.key.jsonName) }
        println("FIELDS " + body.message.fieldsMap.get("fields"))
    };*/
}
