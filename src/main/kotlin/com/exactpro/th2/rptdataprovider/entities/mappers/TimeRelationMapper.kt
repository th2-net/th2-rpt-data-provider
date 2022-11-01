package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException

object TimeRelationMapper {
    fun toHttp(timeRelation: TimeRelation): String {
        return if (timeRelation == TimeRelation.AFTER) "next" else "previous"
    }

    fun fromHttpString(httpDirection: String): TimeRelation {
        if (httpDirection == "next") return TimeRelation.AFTER
        if (httpDirection == "previous") return TimeRelation.BEFORE

        throw InvalidRequestException("'$httpDirection' is not a valid timeline direction. Use 'next' or 'previous'")
    }
}