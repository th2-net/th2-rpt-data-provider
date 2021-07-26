package com.exactpro.th2.rptdataprovider.messageProfile
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.rptdataprovider.entities.responses.Message
import com.exactpro.th2.rptdataprovider.entities.responses.MessageWrapper
import org.apache.log4j.*;
import java.util.concurrent.atomic.AtomicInteger

class MessageProfile(var messageId: StoredMessageId, var messageWrapper: MessageWrapper) {
    private val logger = Logger.getLogger("myappender")
    // время с начала оработки - не обнулять
    var startTimestamp : Long
    // сколько времени заняла обработка
    var resultTime : Long
    // сколько времени какой этап занял
    var stepsInfo : ArrayList<String>
    // состояние сообщения на каждом этапе обработки
    private var messageCondition : ArrayList<String>

    companion object{
        var getMessageCount : AtomicInteger = AtomicInteger(0)
        var inParsingCount : AtomicInteger = AtomicInteger(0)
        var inFilterCount : AtomicInteger = AtomicInteger(0)

        fun getStatistics():ArrayList<String>{
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
    }

    private fun getCurrentTimeMillis() : Long = System.currentTimeMillis()

    /**
     * Обработака сообщения завершена
     * Запись timeStamp-ов на всех этапах обработки
     * */
    fun endOfProcessing() : MessageProfile{
        resultTime = getCurrentTimeMillis() - startTimestamp
        writeResult(resultTime)
        printArrayInLogs(getStatistics(),logger)
        return this
    }

    fun gotMessage(){
        val time = getCurrentTimeMillis() - startTimestamp
        addStepInfo("get message for $messageId", time)
        messageCondition.add("for get message step message $messageId condition is:\n$messageWrapper")
        inParsingCount.incrementAndGet()
        getMessageCount.decrementAndGet()
    }

    /**
     * Сообщение распаршено
     * Отметка времени и сохранение состояния сообщения на этом этапе
     * */
    fun parsed(message : Message){
        val timeForParsing : Long = getCurrentTimeMillis() - startTimestamp
        addStepInfo("parsing for $messageId",timeForParsing)
        messageCondition.add("for parsing step message $messageId condition is:\n$message")
        inParsingCount.decrementAndGet()
        inFilterCount.incrementAndGet()
    }

    /**
     * Сообщение отфильтровано
     * Отметка времени
     * */
    fun filtered(filteredMessage: Message){
        val timeForFilter : Long = getCurrentTimeMillis() - startTimestamp
        addStepInfo("filter for $messageId",timeForFilter)
        messageCondition.add("for filter step message $messageId condition:\n$filteredMessage\n")
        inFilterCount.decrementAndGet()
    }

    /**
     * Сохранение информации о каждом этапе в лист
     * */
    private fun addStepInfo(nameOfStep : String, timeForStep : Long){
        stepsInfo.add("$nameOfStep finished at $timeForStep ms")
    }

    /**
     * Запись окончательного этапа обработки сообщений
     * */
    private fun writeResult(time : Long){
        val result = "Обработка сообщения $messageId заняла $time ms"
        println("Обработка сообщения $messageId заняла $time ms")
        stepsInfo.add(result)
        printArrayInLogs(stepsInfo, logger)
    }
}