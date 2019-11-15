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

package io.siddhi.extension.io.rabbitmq.source;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.rabbitmq.util.RabbitMQSinkUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code RabbitMQConsumer } Handle the Rabbitmq consuming tasks.
 */

public class RabbitMQConsumer {
    private static final Logger log = Logger.getLogger(RabbitMQConsumer.class);

    private Channel channel = null;
    private boolean isPaused;
    private ReentrantLock lock;
    private Condition condition;
    private Source.ConnectionCallback connectionCallback;

    public void consume (Connection connection, String exchangeName, String exchangeType,
                         boolean exchangeDurable, boolean exchangeAutoDelete,
                         String queueName, boolean queueExclusive,
                         boolean queueDurable, boolean queueAutodelete, String routingKey,
                         Map<String, Object> map, SourceEventListener sourceEventListener,
                         Source.ConnectionCallback connectionCallback, boolean autoAck) throws Exception {

        this.connectionCallback = connectionCallback;
        channel = connection.createChannel();
        lock = new ReentrantLock();
        condition = lock.newCondition();
        /*
         * In the following method, system checked whether the exchange.name is already existed or not.
         * If the exchange.name is not existed, then the system declare the exchange.name
         */
        try {
           channel.exchangeDeclarePassive(exchangeName);
        } catch (Exception e) {
            channel = connection.createChannel();
            RabbitMQSinkUtil.declareExchange(connection, channel, exchangeName, exchangeType,
                    exchangeDurable, exchangeAutoDelete);
        }

        if (queueName.isEmpty()) {
            queueName = channel.queueDeclare().getQueue();
        } else {
            /*
             * In the following method, system checked whether the queue.name is already existed or not.
             * If the queue.name is not existed, then the system declare the queue.name
             */
            try {
                channel.queueDeclarePassive(queueName);
            } catch (Exception e) {
                channel = connection.createChannel();
                RabbitMQSinkUtil.declareQueue(connection, channel, queueName, queueDurable,
                        queueAutodelete, queueExclusive);
            }

        }
        channel.queueBind(queueName, exchangeName, routingKey, map);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) {
                 try {
                    if (isPaused) { //spurious wakeup condition is deliberately traded off for performance
                        lock.lock();
                        try {
                            while (!isPaused) {
                                condition.await();
                            }
                        } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        } finally {
                            lock.unlock();
                        }
                    }
                 sourceEventListener.onEvent(body, null);
                 if(!autoAck) {
                	 channel.basicAck(envelope.getDeliveryTag(), false);
                 }
                } catch (Exception e) {
                    log.error("Error in receiving the message from the RabbitMQ broker in "
                        + sourceEventListener, e);
                }
            }
        };

        channel.addShutdownListener(new RabbitMQShutdownListener());
        channel.basicConsume(queueName, autoAck, consumer);
    }


    public void closeChannel() throws IOException, TimeoutException {
        channel.close();
    }

    public void pause() {
        isPaused = true;
    }

    public void resume() {
        isPaused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    class RabbitMQShutdownListener implements ShutdownListener {

        @Override
        public void shutdownCompleted(ShutdownSignalException e) {
            log.error("Exception occurred when consuming messages: " + e.getMessage(), e);
            Thread thread = new Thread() {
                public void run() {
                    connectionCallback.onError(new ConnectionUnavailableException(e));
                }
            };

            thread.start();
        }
    }
}
