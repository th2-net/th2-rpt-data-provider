package com.exactpro.th2.rptdataprovider.messageProfile
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.log4j.Logger
import kotlin.system.measureTimeMillis

class ProfilesStatistics {
    private val logger = Logger.getLogger("worstLogger")
    var enough = false
    // long - processing time in ms
    private var oneSecondWorstList : MutableMap<Long, MessageProfile> = mutableMapOf()
    private var timerThread : Thread? = Thread {
        while (!enough) {
//            Thread.sleep(300)
//            printArrayInLogs(MessageProfile.getStatistics())
            Thread.sleep(1000)
            calculateStatistics()
        }
    }

    companion object{
        @JsonRawValue
        var body: String? = ""

        private var bestProfileInfo : String = ""
        private var worstProfileInfo : String = ""
        private var middleProfileInfo : String = ""

        fun getStatisticsJson() : JsonObject{
            val resultJson = JsonObject()
            val innerJson = JsonObject()
            val counterJson = JsonObject()
            counterJson.addProperty("atGetMessageStage", MessageProfile.getMessageCount)
            counterJson.addProperty("atParsingStage", MessageProfile.inParsingCount)
            counterJson.addProperty("atFilterStage", MessageProfile.inFilterCount)
            innerJson.addProperty("best", bestProfileInfo)
            innerJson.addProperty("middle", middleProfileInfo)
            innerJson.addProperty("worst", worstProfileInfo)
            innerJson.add("counters", counterJson)
            resultJson.add("statistics", innerJson)
            body = resultJson.toString()
            return resultJson
        }
    }

    init {
        timerThread!!.start()
    }

    fun processedMessage(message : MessageProfile){
        oneSecondWorstList[message.resultTime] = message
    }

    private fun bestProfile(){
        val bestTime = oneSecondWorstList.keys.min()!!
        bestProfileInfo = "${oneSecondWorstList[bestTime]!!.messageId} processed for $bestTime"
        writeProfileStatistics("best",bestTime)
    }

    private fun middleProfile(){
        val middleTime = oneSecondWorstList.keys.sum()/oneSecondWorstList.size
        var middle : Long = 0
        oneSecondWorstList.keys.sorted().forEach {
                key->
            run {
                if (key <= middleTime) {
                    middle = key
                }
            }
        }
        middleProfileInfo = "${oneSecondWorstList[middle]!!.messageId} processed for $middle ms"
        writeProfileStatistics("middle",middle)
    }

    private fun worstProfile(){
        val worstTime = oneSecondWorstList.keys.max()!!
        worstProfileInfo = "${oneSecondWorstList[worstTime]!!.messageId} processed for $worstTime ms"
        writeProfileStatistics("worst", worstTime)
    }

    private fun writeProfileStatistics(text:String, time:Long){
        val messageProfile = oneSecondWorstList[time]
        logger.debug("The $text:\n${messageProfile!!.messageId} processing took $time ms")
        printArrayInLogs(messageProfile.stepsInfo, this.logger)
    }

    private fun calculateStatistics(){
        bestProfile()
        middleProfile()
        worstProfile()
    }

    fun enough() {
        //если секунда не прошла, а в листе уже есть сообщения
        if(oneSecondWorstList.isNotEmpty()){
           calculateStatistics()
            enough = true
        }
        enough = true
        timerThread!!.join()
        timerThread = null
    }
}