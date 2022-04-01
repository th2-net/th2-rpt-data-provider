package com.exactpro.th2.rptdataprovider.services

interface DecoderService {
    suspend fun sendToCodec(request: CodecBatchRequest): CodecBatchResponse
}