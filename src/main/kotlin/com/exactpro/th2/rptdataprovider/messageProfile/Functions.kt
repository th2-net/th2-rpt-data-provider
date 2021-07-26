package com.exactpro.th2.rptdataprovider.messageProfile
import org.apache.log4j.Logger

fun printArrayInLogs(array : ArrayList<String>, logger : Logger){
    logger.debug(array.joinToString("\n") + "\n")
}