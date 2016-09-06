/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.carbon.extension.analytics.receiver.rabbitmq.internal.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class RabbitMQUtils {

    private static final Log log = LogFactory.getLog(RabbitMQUtils.class);

    public static Connection createConnection(ConnectionFactory factory) throws IOException {
        return factory.newConnection();
    }

    public static boolean isDurableQueue(String isDurable) {
        return !(isDurable != null && !isDurable.equals("")) || Boolean.parseBoolean(isDurable);
    }

    public static boolean isExclusiveQueue(String isExclusive) {
        return isExclusive != null && !isExclusive.equals("") && Boolean.parseBoolean(isExclusive);
    }

    public static boolean isAutoDeleteQueue(String isAutoDelete) {
        return isAutoDelete != null && !isAutoDelete.equals("") && Boolean.parseBoolean(isAutoDelete);
    }

    public static boolean isQueueAvailable(Connection connection, String queueName) throws IOException {
        Channel channel = connection.createChannel();
        try {
            // check availability of the named queue
            // if an error is encountered, including if the queue does not exist and if the
            // queue is exclusively owned by another connection
            channel.queueDeclarePassive(queueName);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     *
     * @param connection Connection to the RabbitMQ
     * @param queueName Name of the queue
     * @param isDurable Whether durable or not
     * @param isExclusive Whether exclusive or not
     * @param isAutoDelete Whether queue is auto delete or not
     * @throws IOException
     */
    public static void declareQueue(Connection connection, String queueName, String isDurable,
                                    String isExclusive, String isAutoDelete) throws IOException {

        boolean queueAvailable = isQueueAvailable(connection, queueName);
        Channel channel = connection.createChannel();
        if (!queueAvailable) {
            if (log.isDebugEnabled()) {
                log.debug("Queue :" + queueName + " not found or already declared exclusive. Declaring the queue.");
            }
            // Declare the named queue if it does not exists.
            if (!channel.isOpen()) {
                channel = connection.createChannel();
                log.debug("Channel is not open. Creating a new channel.");
            }
            try {
                channel.queueDeclare(queueName, isDurableQueue(isDurable), isExclusiveQueue(isExclusive), isAutoDeleteQueue(isAutoDelete), null);
            } catch (IOException e) {
                handleException("Error while creating queue: " + queueName, e);
            }
        }
    }

    public static void declareExchange(Connection connection, String exchangeName, String exchangeType, String exchangeDurable) throws IOException {
        Boolean exchangeAvailable = false;
        Channel channel = connection.createChannel();
        try {
            // check availability of the named exchange.
            // The server will raise an IOException
            // if the named exchange already exists.
            channel.exchangeDeclarePassive(exchangeName);
            exchangeAvailable = true;
        } catch (IOException e) {
            log.info("Exchange :" + exchangeName + " not found.Declaring exchange.");
        }

        if (!exchangeAvailable) {
            // Declare the named exchange if it does not exists.
            if (!channel.isOpen()) {
                channel = connection.createChannel();
                log.debug("Channel is not open. Creating a new channel.");
            }
            try {
                if (exchangeType != null
                        && !exchangeType.equals("")) {
                    if (exchangeDurable != null && !exchangeDurable.equals("")) {
                        channel.exchangeDeclare(exchangeName,
                                exchangeType,
                                Boolean.parseBoolean(exchangeDurable));
                    } else {
                        channel.exchangeDeclare(exchangeName,
                                exchangeType, true);
                    }
                } else {
                    channel.exchangeDeclare(exchangeName, "direct", true);
                }
            } catch (IOException e) {
                handleException("Error occurred while declaring exchange.", e);
            }
        }
        channel.close();
    }

    public static void handleException(String message, Exception e) {
        log.error(message, e);
        throw new RabbitMQException(message, e);
    }
}