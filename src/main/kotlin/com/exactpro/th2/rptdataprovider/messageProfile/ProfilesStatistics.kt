package com.exactpro.th2.rptdataprovider.messageProfile

import com.exactpro.th2.rptdataprovider.asStringSuspend
import com.exactpro.th2.rptdataprovider.debugArray
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.log4j.Logger

data class ProfilesRangeStatistics(var bestProfile: String, var middleProfile: String, var worstProfile: String) {}

data class ProfileStatisticsData(
    var atGetMessageStage: String, var atParsingStage: String,
    var atFilterStage: String, var profilesRangeStatistics: ProfilesRangeStatistics
) {}

class ProfilesStatistics {
    private val logger = Logger.getLogger("profileStatisticsLogger")
    private var enough = false

    private var oneSecondWorstList: MutableMap<Long, MessageProfile> = mutableMapOf()
    private var timerThread: Job? = null

    var isStarted = false

    fun start() {
        isStarted = true
        timerThread = GlobalScope.launch {
            delay(1000)
            calculateStatistics()
        }
    }

    companion object {
        private var bestProfileInfo: String = ""
        private var worstProfileInfo: String = ""
        private var middleProfileInfo: String = ""

        suspend fun getStatisticsJson(): String {
            val jacksonMapper = ObjectMapper()
            val profilesRangeStatistics = ProfilesRangeStatistics(bestProfileInfo, middleProfileInfo, worstProfileInfo)
            val profileStatisticsData = ProfileStatisticsData(
                MessageProfile.getMessageCount.toString(),
                MessageProfile.getMessageCount.toString(),
                MessageProfile.inFilterCount.toString(),
                profilesRangeStatistics
            )
            return jacksonMapper.asStringSuspend(profileStatisticsData)
        }
    }

    fun processedMessage(message: MessageProfile) {
        oneSecondWorstList[message.resultTime] = message
    }

    private fun bestProfile() {
        val bestTime = oneSecondWorstList.keys.min()!!
        bestProfileInfo = "${oneSecondWorstList[bestTime]!!.messageId} processed for $bestTime"
        writeProfileStatistics("best", bestTime)
    }

    private fun middleProfile() {
        val middleTime = oneSecondWorstList.keys.sum() / oneSecondWorstList.size
        var middle: Long = 0
        oneSecondWorstList.keys.sorted().forEach { key ->
            run {
                if (key <= middleTime) {
                    middle = key
                }
            }
        }
        middleProfileInfo = "${oneSecondWorstList[middle]!!.messageId} processed for $middle ms"
        writeProfileStatistics("middle", middle)
    }

    private fun worstProfile() {
        val worstTime = oneSecondWorstList.keys.max()!!
        worstProfileInfo = "${oneSecondWorstList[worstTime]!!.messageId} processed for $worstTime ms"
        writeProfileStatistics("worst", worstTime)
    }

    private fun writeProfileStatistics(text: String, time: Long) {
        val messageProfile = oneSecondWorstList[time]
        logger.debug("The $text:\n${messageProfile!!.messageId} processing took $time ms")
        logger.debugArray(messageProfile.stepsInfo)
    }

    private fun calculateStatistics() {
        if (oneSecondWorstList.size > 1) {
            bestProfile()
            middleProfile()
            worstProfile()
        }
    }

    suspend fun enough() {
        if (oneSecondWorstList.isNotEmpty()) {
            calculateStatistics()
            enough = true
        }
        enough = true
        timerThread!!.join()
        timerThread = null
    }
}