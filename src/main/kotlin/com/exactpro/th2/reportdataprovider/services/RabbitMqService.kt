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

package com.exactpro.th2.reportdataprovider.services

import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.exactpro.th2.reportdataprovider.entities.configuration.Configuration
import com.google.protobuf.InvalidProtocolBufferException
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging

class RabbitMqService(private val configuration: Configuration) {

    companion object {
        val factory = ConnectionFactory()
        val logger = KotlinLogging.logger { }
    }

    private val decodeRequests = HashSet<Pair<MessageID, Channel<Message>>>()

    private val amqpUri =
        "amqp://${configuration.amqpUsername.value}:${configuration.amqpPassword.value}@${configuration.amqpHost.value}:${configuration.amqpPort.value}/"

    private val connection: Connection? = try {
        factory.newConnection(amqpUri).also { connection ->
            connection.use { usedConnection: Connection ->
                usedConnection.createChannel().use { channel: com.rabbitmq.client.Channel ->

                    val queueName = configuration.amqpProviderQueuePrefix.value + "-IN"

                    channel.queueDeclare(queueName, false, false, true, emptyMap())
                    channel.queueBind(
                        queueName,
                        configuration.amqpCodecExchangeName.value,
                        configuration.amqpCodecRoutingKeyIn.value
                    )

                    channel.basicConsume(
                        queueName,
                        false,
                        configuration.amqpProviderConsumerTag.value,
                        { _, delivery ->
                            try {
                                val decodedBatch = MessageBatch.parseFrom(delivery.body)

                                logger.debug {
                                    val firstId = decodedBatch.messagesList?.first()?.metadata?.id
                                    val session = firstId?.connectionId?.sessionAlias
                                    val direcrion = firstId?.direction?.name
                                    val firstSeqNum = firstId?.sequence
                                    val lastSeqNum = decodedBatch.messagesList?.last()?.metadata?.id?.sequence
                                    val count = decodedBatch.messagesCount

                                    "codec response received (session=$session direction=$direcrion firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
                                }

                                decodedBatch.messagesList.forEach { message ->
                                    val match = decodeRequests.firstOrNull { it.first == message.metadata.id }

                                    if (match != null) {
                                        logger.debug { "response found - ${match.first}" }

                                        match.let { decodeRequests.remove(it) }
                                        GlobalScope.launch { match.second.send(message) }

                                    } else {
                                        logger.warn { "decoded message '${message?.metadata?.id}' was received but no request was found" }
                                    }
                                }

                                channel.basicAck(delivery.envelope.deliveryTag, false)
                            } catch (e: InvalidProtocolBufferException) {
                                logger.error { "unable to parse delivery '${delivery.envelope.deliveryTag}' data:'${delivery.body}'" }
                            }

                        },
                        { consumerTag -> logger.error { "consumer '$consumerTag' was unexpectedly cancelled" } }
                    )
                }
            }
        }
    } catch (e: Exception) {
        logger.error(e) { "unable to establish amqp connection" }
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

            connection.createChannel()
                .use { channel ->
                    channel.basicPublish(
                        configuration.amqpCodecExchangeName.value,
                        configuration.amqpCodecRoutingKeyOut.value,
                        null,
                        batch.toByteArray()
                    )
                }

            logger.debug {
                val firstId = batch.messagesList?.first()?.metadata?.id
                val session = firstId?.connectionId?.sessionAlias
                val direcrion = firstId?.direction?.name
                val firstSeqNum = firstId?.sequence
                val lastSeqNum = batch.messagesList?.last()?.metadata?.id?.sequence
                val count = batch.messagesCount

                "codec request published (session=$session direction=$direcrion firstSeqNum=$firstSeqNum lastSeqNum=$lastSeqNum count=$count)"
            }

            deferred.awaitAll()
        }
    }
}
