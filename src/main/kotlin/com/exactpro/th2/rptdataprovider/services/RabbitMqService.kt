/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.rptdataprovider.services

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.rptdataprovider.entities.configuration.Configuration
import com.google.protobuf.InvalidProtocolBufferException
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging

class RabbitMqService(private val configuration: Configuration) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val decodeRequests = HashSet<Pair<MessageID, Channel<Message>>>()

    private val connection: Connection? =
        try {
            ConnectionFactory().also {
                it.host = configuration.amqpHost.value
                it.port = configuration.amqpPort.value.toInt()
                it.username = configuration.amqpUsername.value
                it.password = configuration.amqpPassword.value
                it.virtualHost = configuration.amqpVhost.value
            }.newConnection()
        } catch (e: Exception) {
            logger.error(e) { "unable to establish amqp connection" }
            null
        }

    private val receiveChannel: com.rabbitmq.client.Channel? =
        try {
            connection?.createChannel()?.also { channel ->

                val queueName = configuration.amqpProviderQueuePrefix.value + "-IN"

                channel.queueDeclare(queueName, false, false, true, emptyMap())
                channel.queueBind(
                    queueName,
                    configuration.amqpCodecExchangeName.value,
                    configuration.amqpCodecRoutingKeyIn.value
                )

                logger.debug { "receive queue '$queueName' is bound to routing key '${configuration.amqpCodecRoutingKeyIn.value}'" }

                channel.basicConsume(
                    queueName,
                    false,
                    configuration.amqpProviderConsumerTag.value,
                    { _, delivery ->
                        try {
                            val decodedBatch = MessageBatch.parseFrom(delivery.body)

                            var needAck:Boolean = false
                            decodedBatch.messagesList.forEach { message ->
                                val match = decodeRequests.firstOrNull { it.first == message.metadata.id }

                                if (match != null) {
                                    match.let { decodeRequests.remove(it) }
                                    GlobalScope.launch { match.second.send(message) }
                                    needAck = true
                                } else {
                                    logger.warn {
                                        val id = message?.metadata?.id

                                        val idString =
                                            "(stream=${id?.connectionId?.sessionAlias}, direction=${id?.direction}, sequence=${id?.sequence})"

                                        "decoded message '$idString' was received but no request was found"
                                    }
                                }
                            }

                            if (needAck) {
                                channel.basicAck(delivery.envelope.deliveryTag, false)
                            }

                            if (decodeRequests.size > 0) {
                                logger.debug { "${decodeRequests.size} decode requests remaining" }
                            }

                        } catch (e: InvalidProtocolBufferException) {
                            logger.error { "unable to parse delivery '${delivery.envelope.deliveryTag}' data:'${delivery.body}'" }
                        }

                    },
                    { consumerTag -> logger.error { "consumer '$consumerTag' was unexpectedly cancelled" } }
                )
            }
        } catch (e: Exception) {
            logger.error(e) { "unable to create a receive channel" }
            null
        }

    suspend fun decodeMessage(batch: RawMessageBatch): Collection<Message> {
        if (connection == null) {
            return listOf()
        }

        val requests: Set<Pair<MessageID, Channel<Message>>> = batch.messagesList
            .map { it.metadata.id to Channel<Message>(0) }
            .toSet()

        return withContext(Dispatchers.IO) {
            val deferred = requests.map { async { it.second.receive() } }

            decodeRequests.addAll(requests)

            connection.createChannel()
                .use { channel ->
                    channel.basicPublish(
                        configuration.amqpCodecExchangeName.value,
                        configuration.amqpCodecRoutingKeyOut.value,
                        null,
                        batch.toByteArray()
                    )
                }

            val requestDebugInfo = let {
                val firstId = batch.messagesList?.first()?.metadata?.id
                val session = firstId?.connectionId?.sessionAlias
                val direction = firstId?.direction?.name
                val firstSeqNum = firstId?.sequence
                val lastSeqNum = batch.messagesList?.last()?.metadata?.id?.sequence
                val count = batch.messagesCount

                "(session=$session direction=$direction firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
            }

            logger.debug { "codec request published $requestDebugInfo" }

            deferred.awaitAll().also { logger.debug { "codec response received $requestDebugInfo" } }
        }
    }
}
