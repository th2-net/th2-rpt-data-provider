/*******************************************************************************
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction
import com.exactpro.th2.common.util.toCradleDirection
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException

data class StreamName(val name: String, val direction: Direction) {
    companion object {
        private val separator = ":"
        private fun check(value: List<String>) {
            val lastIsDirectionValue =
                Direction.values().map { it.name.toLowerCase() }.contains(value.last().toLowerCase())
            if (!(value.size >= 2 && lastIsDirectionValue)) {
                throw InvalidRequestException("Incorrect message stream ${value}. Correct view is 'name:direction'")
            }
        }

        private fun getName(value: String): String {
            return value.split(separator).let {
                check(it)
                value.substring(0, value.lastIndexOf(separator))
            }
        }

        private fun getDirection(value: String): Direction {
            return value.split(separator).let {
                check(it)
                Direction.byLabel(it.last().toLowerCase())
            }
        }
    }

    private val fullName = "$name:$direction"

    override fun toString(): String {
        return fullName
    }

    constructor(messageStream: MessageStream) : this(
        name = messageStream.name,
        direction = messageStream.direction.toCradleDirection()
    )

    constructor(messageStream: String) : this(
        name = getName(messageStream),
        direction = getDirection(messageStream)
    )
}
