/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.wso2.extension.siddhi.io.rabbitmq.sink;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class RabbitMQSinkTestUtil {
    private static final Logger log = Logger.getLogger(RabbitMQSinkTestUtil.class);
    private static Connection connection = null;
    private static Channel channel = null;
    private static String queueName;
    private static boolean eventArrived;
    private static int count;
    private static Map<String, Object> map = null;

    public static Channel createConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        URI uri = URI.create("amqp://guest:guest@172.17.0.2:5672");
        factory.setUri(uri);
        connection = factory.newConnection();
        return (Channel) connection.createChannel();
    }

    public static void consumer (String exchangeName, String exchangeType, boolean exchangeDurable,
                                 boolean exchangeAutodelete, String routingKey, boolean eventArrive, int counter)
            throws Exception {
        count = counter;
        eventArrived = eventArrive;
        channel = (Channel) createConnection();
        boolean exchangeAvailable = isExchangeAvailable(exchangeName);
        if (!exchangeAvailable) {
            if (!channel.isOpen()) {
                channel = (Channel) createConnection();
                log.debug("Channel is not open. Creating a new channel.");
            }
            channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable, exchangeAutodelete, null);
        }

       if (exchangeType.equals("headers")) {
           map = new HashMap<String, Object>();
           map.put("x-match", "any");
           map.put("A", "1");
           map.put("C", "3");
       }
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeName, routingKey, map);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                eventArrived = true;
                count++;
                String message = new String(body, "UTF-8");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    public static boolean isExchangeAvailable(String exchangeName) throws Exception {
        channel = (Channel) createConnection();
        try {
            channel.exchangeDeclarePassive(exchangeName);
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    public static int getCount() {
        return count;
    }

    public static boolean geteventArrived() {
        return eventArrived;
    }
}
