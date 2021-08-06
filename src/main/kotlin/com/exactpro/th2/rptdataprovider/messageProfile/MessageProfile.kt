package com.exactpro.th2.rptdataprovider.messageProfile

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.debugArray
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import org.apache.log4j.*
import java.util.concurrent.atomic.AtomicInteger

class MessageProfile(var messageId: StoredMessageId, var messageWrapper: MessageWrapper) {
    private val logger = Logger.getLogger("myappender")

    var startTimestamp: Long

    var resultTime: Long

    var stepsInfo: ArrayList<String>

    private var messageCondition: ArrayList<String>

    companion object {
        var getMessageCount: AtomicInteger = AtomicInteger(0)
        var inParsingCount: AtomicInteger = AtomicInteger(0)
        var inFilterCount: AtomicInteger = AtomicInteger(0)

        fun getStatistics(): ArrayList<String> {
            val list = arrayListOf<String>()
            list.add("At get massage stage: $getMessageCount")
            list.add("At parsing stage: $inParsingCount")
            list.add("At filter stage: $inFilterCount")
            return list
        }
    }

    init {
        startTimestamp = getCurrentTimeMillis()
        stepsInfo = ArrayList()
        messageCondition = ArrayList()
        resultTime = 0
        getMessageCount.incrementAndGet()
    }

    private fun getCurrentTimeMillis(): Long = System.currentTimeMillis()

    fun endOfProcessing(): MessageProfile {
        resultTime = getCurrentTimeMillis() - startTimestamp
        writeResult(resultTime)
        logger.debugArray(getStatistics())
        return this
    }

    fun gotMessage() {
        val time = getCurrentTimeMillis() - startTimestamp
        addStepInfo("get message for $messageId", time)
        messageCondition.add("for get message step message $messageId condition is:\n$messageWrapper")
        inParsingCount.incrementAndGet()
        getMessageCount.decrementAndGet()
    }

    fun parsed(message: Message) {
        val timeForParsing: Long = getCurrentTimeMillis() - startTimestamp
        addStepInfo("parsing for $messageId", timeForParsing)
        messageCondition.add("for parsing step message $messageId condition is:\n$message")
        inParsingCount.decrementAndGet()
        inFilterCount.incrementAndGet()
    }

    fun filtered(filteredMessage: Message) {
        val timeForFilter: Long = getCurrentTimeMillis() - startTimestamp
        addStepInfo("filter for $messageId", timeForFilter)
        messageCondition.add("for filter step message $messageId condition:\n$filteredMessage\n")
        inFilterCount.decrementAndGet()
    }

    private fun addStepInfo(nameOfStep: String, timeForStep: Long) {
        stepsInfo.add("$nameOfStep finished at $timeForStep ms")
    }

    private fun writeResult(time: Long) {
        val result = "Обработка сообщения $messageId заняла $time ms"
        println("Обработка сообщения $messageId заняла $time ms")
        stepsInfo.add(result)
        logger.debugArray(stepsInfo)
    }
}