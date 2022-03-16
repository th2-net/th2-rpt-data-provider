package com.exactpro.th2.rptdataprovider.entities.mappers

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.rptdataprovider.entities.exceptions.InvalidRequestException

object TimeRelationMapper {
    fun toHttp(timeRelation: TimeRelation): String {
        return if (timeRelation == TimeRelation.AFTER) "next" else "previous"
    }

    fun fromHttpString(httpValue: String): TimeRelation {
        if (httpValue == "next") return TimeRelation.AFTER
        if (httpValue == "previous") return TimeRelation.BEFORE

        throw InvalidRequestException("'$this' is not a valid timeline direction. Use 'next' or 'previous'")
    }
}