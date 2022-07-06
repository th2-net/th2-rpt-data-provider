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

package com.exactpro.th2.rptdataprovider

import com.exactpro.cradle.Direction
import java.time.Instant
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

fun cradleDirectionToGrpc(direction: Direction): com.exactpro.th2.common.grpc.Direction {
    return if (direction == Direction.FIRST)
        com.exactpro.th2.common.grpc.Direction.FIRST
    else
        com.exactpro.th2.common.grpc.Direction.SECOND
}

fun grpcDirectionToCradle(direction: com.exactpro.th2.common.grpc.Direction): Direction {
    return if (direction == com.exactpro.th2.common.grpc.Direction.FIRST)
        Direction.FIRST
    else
        Direction.SECOND
}


fun isDifferentDays(from: Instant, to: Instant): Boolean {
    val fromDay = from.atOffset(ZoneOffset.UTC).dayOfYear
    val toDay = to.atOffset(ZoneOffset.UTC).dayOfYear
    return fromDay != toDay
}


fun getDayStart(instant: Instant): OffsetDateTime {
    return instant
        .atOffset(ZoneOffset.UTC)
        .with(LocalTime.of(0, 0, 0, 0))
}