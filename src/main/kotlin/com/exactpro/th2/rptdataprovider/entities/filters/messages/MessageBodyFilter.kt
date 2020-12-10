package com.exactpro.th2.rptdataprovider.entities.filters.messages

import com.exactpro.th2.rptdataprovider.entities.filters.Filter
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.rptdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.rptdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.services.cradle.CradleService

class MessageBodyFilter(
    requestMap: Map<String, List<String>>,
    cradleService: CradleService
) : Filter<Message>(requestMap, cradleService) {

    private var body: List<String>
    override var negative: Boolean = false

    init {
        negative = requestMap["${filterInfo.name}-negative"]?.first()?.toBoolean() ?: false
        body = requestMap["${filterInfo.name}-values"]!!
    }

    companion object {
        val filterInfo = FilterInfo(
            "body",
            "matches messages whose body contains one of the specified tokens",
            mutableListOf<Parameter>().apply {
                add(Parameter("invert", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "FGW, ..."))
            }
        )
    }

    override fun match(element: Message): Boolean {
        return negative.xor(body.any { item ->
            element.body?.toLowerCase()?.contains(item.toLowerCase()) ?: false
        })
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }
}